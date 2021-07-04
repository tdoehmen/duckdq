from typing import Set, Sequence, Dict
import multiprocessing

from datasketches import kll_ints_sketch, kll_floats_sketch
from datasketches import hll_sketch, hll_union, tgt_hll_type
from pandas import DataFrame
from pandas.core.dtypes.common import is_numeric_dtype, is_string_dtype

import numpy as np
from duckdq.core.metrics import Metric
from duckdq.core.preconditions import Precondition, HasColumn, IsNumeric, IsString, AtLeastOne
from duckdq.engines.pandas import sketch_utils
from duckdq.engines.pandas.sketch_utils import DEFAULT_SKETCH_SIZE, DEFAULT_HLL_K, DEFAULT_HLL_TYPE
from duckdq.metadata.metadata_repository import MetadataRepository
from duckdq.core.states import QuantileState, ApproxDistinctState
from duckdq.core.properties import Property, Quantile, ApproxDistinctness
from duckdq.engines.engine import Engine
from duckdq.engines.sql.sql_engine import DuckDBEngine
from duckdq.utils.connection_handler import ConnectionHandler
from duckdq.utils.exceptions import PreconditionNotMetException

from duckdq.utils.metrics_helper import metric_from_value


class PandasEngine(Engine):
    def __init__(self, data: DataFrame, no_sharing = False):
        super().__init__()
        con = ConnectionHandler.get_connection("duckdb://:memory:")

        # activate multiprocessing with available cpu's but one
        con.execute(f"PRAGMA threads={multiprocessing.cpu_count()}")

        # register df
        con.register("data", data)

        self.engine = DuckDBEngine(con, "data", no_sharing=no_sharing)
        self.data = data
        self.table = "data"

    def profile(self):
        core_profile = self.engine.profile(with_quantiles=False)
        stats = core_profile["stats"]

        sketch_statistics = sketch_utils.calculate_sketch_statistics(self.data)

        for col, stat in stats.items():
            if col in sketch_statistics.keys():
                stat += sketch_statistics[col]

        return core_profile

    def evaluate_preconditions(self, preconditions: Sequence[Precondition]):
        for cond in preconditions:
            if isinstance(cond, HasColumn) and cond.column not in self.data.columns:
                raise PreconditionNotMetException(f"Input data does not include column {cond.column}")
            elif isinstance(cond, IsNumeric) and not is_numeric_dtype(self.data[cond.column]):
                raise PreconditionNotMetException(f"Expected type of column {cond.column} to be"
                                                  f" of numeric type,"
                                                  f" but found {self.data[cond.column].dtype} instead!")
            elif isinstance(cond, IsString) and not is_string_dtype(self.data[cond.column]):
                raise PreconditionNotMetException(f"Expected type of column {cond.column} to be"
                                                  f" of string type,"
                                                  f" but found {self.data[cond.column].dtype} instead!")
            elif isinstance(cond, AtLeastOne) and len(cond.columns) < 1:
                raise PreconditionNotMetException("At least one column needs to be specified!")

    def compute_metrics(self, properties: Set[Property], repo: MetadataRepository):
        quantile_properties = [property for property in properties if isinstance(property, Quantile)]
        quantile_metrics: Dict[Property, Metric] = {}
        for quantile_property in quantile_properties:
            data_col = self.data[quantile_property.column].to_numpy()
            sketch_type = ""
            if self.data[quantile_property.column].dtype == np.int64:
                kll = kll_ints_sketch(DEFAULT_SKETCH_SIZE)
                sketch_type = "ints"
            elif self.data[quantile_property.column].dtype == np.float64:
                kll = kll_floats_sketch(DEFAULT_SKETCH_SIZE)
                sketch_type = "floats"
            else:
                raise NotImplementedError(f"Data Type {self.data[quantile_property.column].dtype} is not supported for sketches!")
            kll.update(data_col)
            quantile = kll.get_quantiles([quantile_property.quantile])[0]
            serialized_kll = kll.serialize().hex() #bytes.fromhex()
            quantile_state = QuantileState(quantile_property.property_identifier(), serialized_kll, quantile, sketch_type)
            repo.register_state(quantile_state)
            quantile_metric = metric_from_value(
                quantile, quantile_property.name, quantile_property.instance, quantile_property.entity
            )
            quantile_metrics[quantile_property] = quantile_metric

        approx_distinct_properties = [property for property in properties if isinstance(property, ApproxDistinctness)]
        approx_distinct_metrics: Dict[Property, Metric] = {}
        for approx_distinct_property in approx_distinct_properties:
            data_col = self.data[approx_distinct_property.column].to_numpy()
            hll = hll_sketch(DEFAULT_HLL_K, DEFAULT_HLL_TYPE)
            #for v in data_col: #slow
            #    hll.update(v)
            hll.update(data_col) #works with local fork (np.array extension)
            approx_distinct_count = hll.get_estimate()
            num_rows = len(data_col)
            serialized_hll = hll.serialize_updatable().hex() #bytes.fromhex()
            approx_distinct_state = ApproxDistinctState(approx_distinct_property.property_identifier(), serialized_hll, approx_distinct_count, num_rows)
            repo.register_state(approx_distinct_state)
            approx_distinctness = min(approx_distinct_count/num_rows, 1.00)
            approx_distinct_metric = metric_from_value(
                approx_distinctness, approx_distinct_property.name, approx_distinct_property.instance, approx_distinct_property.entity
            )
            approx_distinct_metrics[approx_distinct_property] = approx_distinct_metric

        other_properties = [property for property in properties if (not isinstance(property, Quantile) and not isinstance(property, ApproxDistinctness))]
        metrics = self.engine.compute_metrics(other_properties, repo)
        metrics.update(quantile_metrics)
        metrics.update(approx_distinct_metrics)
        return metrics
