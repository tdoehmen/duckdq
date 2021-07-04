import importlib
import json

import duckdb
import math
import dill
from typing import Dict, Sequence, List

from datasketches import kll_floats_sketch, kll_ints_sketch, hll_sketch
from duckdb import DuckDBPyConnection

from duckdq import VerificationSuite
from duckdq.core.metrics import Metric
from duckdq.core.properties import Property, Quantile, ApproxDistinctness, Schema
from duckdq.core.states import State, SchemaState, MaxState, MeanState, MinState, NumMatches, NumMatchesAndCount, \
    QuantileState, ApproxDistinctState, StandardDeviationState, SumState, FrequenciesAndNumRows
from duckdq.engines.pandas.sketch_utils import DEFAULT_SKETCH_SIZE
from duckdq.engines.sql.sql_operators import SQLOperatorFactory
from duckdq.utils.analysis_runner import AnalyzerContext
from duckdq.utils.metrics_helper import metric_from_value
from duckdq.verification_suite import VerificationResult


class StateEngine:
    def __init__(self, con: DuckDBPyConnection):
        self.con = con
        super().__init__()

    def get_validation_runs(self) -> Sequence[Dict]:
        val_runs_df = self.con.execute("SELECT run_id, ts, dataset_id, partition_id, check_serialized FROM dq_checks JOIN dq_validation_runs USING(run_id)").fetchdf()
        val_runs = val_runs_df.to_dict(orient='records')
        for val_run in val_runs:
            check = dill.loads(bytes.fromhex(val_run['check_serialized']))
            val_run['check'] = check
            val_run['required_properties'] = check.required_analyzers()
        return val_runs

    def merge_states(self, run_ids: Sequence[str], required_properties: Sequence[Property]) -> Dict[Property, State]:
        property_states_map: Dict[Property, Sequence[State]] = {p:[] for p in required_properties}
        required_properties_map = {str(required_property.property_identifier()): required_property for required_property in required_properties}
        for run_id in run_ids:
            states_df = self.con.execute(f"SELECT run_id, property_id, state_type, state_serialized from dq_states JOIN dq_validation_runs USING(run_id) WHERE run_id='{run_id}'").fetchdf()
            states = states_df.to_dict(orient='records')
            for state in states:
                property_id = state['property_id']
                if property_id not in list(required_properties_map.keys()):
                    continue
                property = required_properties_map[property_id]
                state_type = state['state_type']
                state_dict = json.loads(state["state_serialized"])
                StateClass = getattr(importlib.import_module("duckdq.core.states"), state_type)
                property_states_map[property].append(StateClass(**state_dict))

        property_state_map: Dict[Property, State] = {}
        for property, states in property_states_map.items():
            state = self.__merge_states(states)
            #state = states[0]
            property_state_map[property] = state

        return property_state_map

    def __merge_states(self, states: Sequence[State]) -> State:
        first_state = states[0]
        result_state = None
        if isinstance(first_state,SchemaState):
            result_state = first_state
        elif isinstance(first_state,MaxState):
            max_value: float = first_state.max_value
            for state in states:
                max_value = max(max_value,state.max_value)
            result_state = MaxState(first_state.id,max_value)
        elif isinstance(first_state,MeanState):
            total: float = 0
            count: int = 0
            for state in states:
                total = total+state.total
                count = count+state.count
            result_state = MeanState(first_state.id,total,count)
        elif isinstance(first_state,MinState):
            min_value: float = first_state.min_value
            for state in states:
                min_value = min(min_value, state.min_value)
            result_state = MinState(first_state.id,min_value)
        elif isinstance(first_state,NumMatches):
            num_matches: int = 0
            for state in states:
                num_matches = num_matches+state.num_matches
            result_state = NumMatches(first_state.id, num_matches)
        elif isinstance(first_state,NumMatchesAndCount):
            num_matches: int = 0
            count: int = 0
            for state in states:
                num_matches = num_matches+state.num_matches
                count = count+state.count
            result_state = NumMatchesAndCount(first_state.id, num_matches, count)
        elif isinstance(first_state,QuantileState):
            if first_state.sketch_type == "floats":
                kll_ser = kll_floats_sketch(DEFAULT_SKETCH_SIZE)
            else:
                kll_ser = kll_ints_sketch(DEFAULT_SKETCH_SIZE)
            main_kll = kll_ser.deserialize(bytes.fromhex(first_state.serializedKll))

            i = 0
            for state in states:
                if i == 0:
                    i += 1
                    continue
                new_kll = kll_ser.deserialize(bytes.fromhex(state.serializedKll))
                main_kll.merge(new_kll)

            result_state = QuantileState(first_state.id, main_kll.serialize().hex(), first_state.quantile, first_state.sketch_type)
        elif isinstance(first_state,ApproxDistinctState):
            main_hll = hll_sketch.deserialize(bytes.fromhex(first_state.serializedHll))
            num_rows = first_state.num_rows
            i = 0
            for state in states:
                if i == 0:
                    i += 1
                    continue
                num_rows = num_rows + state.num_rows
                new_hll = hll_sketch.deserialize(bytes.fromhex(state.serializedHll))
                main_hll.update(new_hll)
            approx_distinct_count = main_hll.get_estimate()
            serialized_hll = main_hll.serialize_updatable().hex()
            result_state = ApproxDistinctState(first_state.id, serialized_hll, approx_distinct_count, num_rows)
        elif isinstance(first_state,StandardDeviationState):
            target_n: float = first_state.n
            target_avg: float = first_state.avg
            target_m2: float = first_state.m2
            stddev: float = first_state.stddev
            i = 0
            for state in states:
                if i == 0:
                    i += 1
                    continue
                new_n = target_n + state.n;
                new_avg = (state.n * state.avg + target_n * target_avg) / new_n
                delta = state.avg - target_avg
                target_m2 = state.m2 + target_m2 + delta * delta * state.n * target_n / new_n
                target_avg = new_avg
                target_n = new_n
            target_stddev = math.sqrt(target_m2 / (target_n - 1)) if target_n > 1 else 0
            result_state = StandardDeviationState(first_state.id, target_n, target_avg, target_m2, target_stddev)
        elif isinstance(first_state,SumState):
            sum_value: float = 0
            for state in states:
                sum_value = sum_value + state.sum_value
            result_state = SumState(first_state.id, sum_value)
        elif isinstance(first_state,FrequenciesAndNumRows):
            raise NotImplementedError("Merging of FrequenciesAndNumRows states not implemented, yet")
            #frequencies_table: str
            #grouping_columns: List[str]
            #num_rows: int
            #def get_table_name(self) -> str:
            #    return self.frequencies_table

        return result_state


    def metrics_from_states(self, properties_and_states: Dict[Property, State]) -> Dict[Property, Metric]:
        property_metric_map: Dict[Property, Metric] = {}
        for prop, state in properties_and_states.items():
            if isinstance(prop,Quantile):
                quantile_state = state#QuantileState(quantile_property.property_identifier(), serialized_kll, quantile)
                if state.sketch_type == "floats":
                    kll_ser = kll_floats_sketch(DEFAULT_SKETCH_SIZE)
                else:
                    kll_ser = kll_ints_sketch(DEFAULT_SKETCH_SIZE)
                main_kll = kll_ser.deserialize(bytes.fromhex(state.serializedKll))
                quantile = main_kll.get_quantiles([prop.quantile])[0]
                quantile_metric = metric_from_value(
                    quantile, prop.name, prop.instance, prop.entity
                )
                property_metric_map[prop] = quantile_metric
            elif isinstance(prop,ApproxDistinctness):
                approx_distinct_state = state#ApproxDistinctState(approx_distinct_property.property_identifier(), serialized_hll, approx_distinct_count, num_rows)
                approx_distinctness = min(approx_distinct_state.approx_distinct_count/approx_distinct_state.num_rows, 1.00)
                approx_distinct_metric = metric_from_value(
                    approx_distinctness, prop.name, prop.instance, prop.entity
                )
                property_metric_map[prop] = approx_distinct_metric
            elif isinstance(prop,Schema):
                schema_state = state#SchemaState(schema_property.property_identifier(),schema)
                schema = schema_state.schema
                schema_metric = metric_from_value(
                    schema, prop.name, prop.instance,prop.entity
                )
                property_metric_map[prop] = schema_metric
            else:
                operator = SQLOperatorFactory.create_operator(prop)
                metric = operator.get_metric(state)
                property_metric_map[prop] = metric

        return property_metric_map