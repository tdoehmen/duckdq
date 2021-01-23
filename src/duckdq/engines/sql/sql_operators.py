import json
from abc import ABCMeta, abstractmethod
from typing import List, FrozenSet, Optional

from pandas import DataFrame

from duckdq.core.metrics import Metric, DoubleMetric
from duckdq.utils.metrics_helper import metric_from_value
from duckdq.core.properties import Property, Completeness, Distinctness, Uniqueness, Schema, Size, \
    HistogramProperty, UniqueValueRatio, Compliance, PatternMatch, Entropy, Quantile, MaxLength, \
    MinLength, Minimum, Maximum, Mean, Sum, StandardDeviation, Correlation, KllSketch, ApproxDistinctness
from duckdq.core.states import State, NumMatchesAndCount, FrequenciesAndNumRows, NumMatches, MaxState, MinState, \
    MeanState, SumState, StandardDeviationState
from duckdq.utils.exceptions import UnsupportedPropertyException

class SQLOperator(metaclass=ABCMeta):

    def __init__(self, property: Property):
        self.property = property

    @abstractmethod
    def get_property(self) -> Property:
        pass

class ScanShareableOperator(SQLOperator):

    def __init__(self, property: Property, aggregations: List[str]):
        super().__init__(property)
        self.aggregations = frozenset(aggregations)

    def get_aggregations(self) -> FrozenSet[str]:
        return self.aggregations

    @abstractmethod
    def extract_state(self, result: DataFrame) -> State:
        pass

    @abstractmethod
    def get_metric(self, state: State) -> Metric:
        pass

class GroupingShareableOperator(SQLOperator):

    def __init__(self, property: Property, aggregations: List[str], groupings: List[str], filter: Optional[str] = None):
        super().__init__(property)
        self.aggregations = frozenset(aggregations)
        self.groupings = frozenset(groupings)
        self.filter = filter
        self.num_rows = f"count{self.property.filter_identifier()}"

    def get_aggregations(self) -> FrozenSet[str]:
        return self.aggregations

    def get_groupings(self) -> FrozenSet[str]:
        return self.groupings

    def get_num_rows(self) -> str:
        return self.num_rows

    @abstractmethod
    def extract_metric(self, result: DataFrame, num_rows: int) -> State:
        pass

class CompletenessOperator(ScanShareableOperator):

    def __init__(self, property: Completeness):
        self.count_column = f"count{property.filter_identifier()}"
        self.count_na_column = f"{property.column}_count_na{property.property_identifier()}".lower()

        if property.where is None:
            aggregations = [f"COUNT(*) as {self.count_column}",
                           f"SUM(CASE WHEN {property.column} IS NULL THEN 1 ELSE 0 END) as {self.count_na_column}"]
        else:
            aggregations = [f"SUM(CASE WHEN {property.where} THEN 1 ELSE 0 END) as {self.count_column}",
                            f"SUM(CASE WHEN ({property.where}) AND ({property.column}) IS NULL THEN 1 ELSE 0 END) as {self.count_na_column}"]

        super().__init__(property, aggregations)

    def get_property(self) -> Completeness:
        return self.property

    def extract_state(self, result_df: DataFrame) -> NumMatchesAndCount:
        count = int(result_df[self.count_column][0])
        count_na = int(result_df[self.count_na_column][0])
        return NumMatchesAndCount(self.property.property_identifier(), count-count_na, count)

    def get_metric(self, state: NumMatchesAndCount) -> DoubleMetric:
        return metric_from_value(
            state.num_matches/state.count, self.property.name, self.property.instance, self.property.entity
        )


class DistinctnessOperator(GroupingShareableOperator):

    def __init__(self, property: Distinctness):
        self.distinct_count = f"distinct_count"
        aggregations = [f"SUM(CASE WHEN count >= 1 THEN 1 ELSE 0 END) as {self.distinct_count}"]
        super().__init__(property, aggregations, property.columns, property.where)

    def get_property(self) -> Distinctness:
        return self.property

    def extract_metric(self, result: DataFrame, num_rows: int) -> DoubleMetric:
        distinct_count = int(result[self.distinct_count][0])
        return metric_from_value(
            distinct_count/num_rows, self.property.name, self.property.instance, self.property.entity
        )


class UniquenessOperator(GroupingShareableOperator):

    def __init__(self, property: Uniqueness):
        self.unique_count = f"unique_count"
        aggregations = [f"SUM(CASE WHEN count = 1 THEN 1 ELSE 0 END) as {self.unique_count}"]
        super().__init__(property, aggregations, property.columns, property.where)

    def get_property(self) -> Uniqueness:
        return self.property

    def extract_metric(self, result: DataFrame, num_rows: int) -> DoubleMetric:
        uniqueness_count = int(result[self.unique_count][0])
        return metric_from_value(
            uniqueness_count/num_rows, self.property.name, self.property.instance, self.property.entity
        )


class SizeOperator(ScanShareableOperator):
    def __init__(self, property: Size):
        self.count_column = f"count{property.filter_identifier()}"

        if property.where is None:
            aggregations = [f"COUNT(*) as {self.count_column}"]
        else:
            aggregations = [f"SUM(CASE WHEN {property.where} THEN 1 ELSE 0 END) as {self.count_column}"]

        super().__init__(property, aggregations)

    def get_property(self) -> Size:
        return self.property

    def extract_state(self, result_df: DataFrame) -> NumMatches:
        count = int(result_df[self.count_column][0])
        return NumMatches(self.property.property_identifier(), count)

    def get_metric(self, state: NumMatches) -> DoubleMetric:
        return metric_from_value(
            state.num_matches, self.property.name, self.property.instance, self.property.entity
        )


class UniqueValueRatioOperator(GroupingShareableOperator):
    def __init__(self, property: UniqueValueRatio):
        self.distinct_count = f"distinct_count"
        self.unique_count = f"unique_count"
        aggregations = [f"SUM(CASE WHEN count >= 1 THEN 1 ELSE 0 END) as {self.distinct_count}",
                        f"SUM(CASE WHEN count = 1 THEN 1 ELSE 0 END) as {self.unique_count}"]
        super().__init__(property, aggregations, property.columns, property.where)

    def get_property(self) -> UniqueValueRatio:
        return self.property

    def extract_metric(self, result: DataFrame, num_rows: int) -> DoubleMetric:
        distinct_n = int(result[self.distinct_count][0])
        unique_n = int(result[self.unique_count][0])
        return metric_from_value(
            unique_n/distinct_n, self.property.name, self.property.instance, self.property.entity
        )


class ComplianceOperator(ScanShareableOperator):

    def __init__(self, property: Compliance):
        self.count_column = f"count{property.filter_identifier()}"
        self.count_compliance_column = f"count_compliance{property.property_identifier()}".lower()

        if property.where is None:
            aggregations = [f"COUNT(*) as {self.count_column}",
                            f"SUM(CASE WHEN ({property.expression}) THEN 1 ELSE 0 END) as {self.count_compliance_column}"]
        else:
            aggregations = [f"SUM(CASE WHEN ({property.where}) THEN 1 ELSE 0 END) as {self.count_column}",
                            f"SUM(CASE WHEN ({property.where}) AND ({property.expression}) THEN 1 ELSE 0 END) as {self.count_compliance_column}"]

        super().__init__(property, aggregations)

    def get_property(self) -> Compliance:
        return self.property

    def extract_state(self, result_df: DataFrame) -> NumMatchesAndCount:
        count = int(result_df[self.count_column][0])
        count_compliance_column = int(result_df[self.count_compliance_column][0])
        return NumMatchesAndCount(self.property.property_identifier(), count_compliance_column, count)

    def get_metric(self, state: NumMatchesAndCount) -> DoubleMetric:
        return metric_from_value(
            state.num_matches/float(state.count), self.property.name, self.property.instance, self.property.entity
        )


class PatternMatchOperator(ScanShareableOperator):
    def __init__(self, property: PatternMatch):
        self.count_column = f"count{property.filter_identifier()}"
        self.count_pattern_match_column = f"count_pattern_match{property.property_identifier()}".lower()

        if property.where is None:
            aggregations = [f"COUNT(*) as {self.count_column}",
                            f"SUM(CASE WHEN regexp_full_match({property.column},'{property.pattern}') THEN 1 ELSE 0 END) as {self.count_pattern_match_column}"]
        else:
            aggregations = [f"SUM(CASE WHEN ({property.where}) THEN 1 ELSE 0 END) as {self.count_column}",
                            f"SUM(CASE WHEN ({property.where}) AND (regexp_full_match({property.column},'{property.pattern}')) THEN 1 ELSE 0 END) as {self.count_pattern_match_column}"]

        super().__init__(property, aggregations)

    def get_property(self) -> Compliance:
        return self.property

    def extract_state(self, result_df: DataFrame) -> NumMatchesAndCount:
        count = int(result_df[self.count_column][0])
        count_pattern_match = int(result_df[self.count_pattern_match_column][0])
        return NumMatchesAndCount(self.property.property_identifier(), count_pattern_match, count)

    def get_metric(self, state: NumMatchesAndCount) -> DoubleMetric:
        return metric_from_value(
            state.num_matches/float(state.count), self.property.name, self.property.instance, self.property.entity
        )

class MaxLengthOperator(ScanShareableOperator):

    def __init__(self, property: MaxLength):
        self.max_length_col = f"max_len{property.property_identifier()}"

        if property.where is None:
            aggregations = [f"max(length({property.column})) as {self.max_length_col}"]
        else:
            aggregations = [f"max(CASE WHEN {property.where} THEN length({property.column}) ELSE NULL END) as {self.max_length_col}"]

        super().__init__(property, aggregations)

    def get_property(self) -> MaxLength:
        return self.property

    def extract_state(self, result_df: DataFrame) -> MaxState:
        max_length = int(result_df[self.max_length_col][0])
        return MaxState(self.property.property_identifier(), max_length)

    def get_metric(self, state: MaxState) -> DoubleMetric:
        return metric_from_value(
            state.max_value, self.property.name, self.property.instance, self.property.entity
        )


class MinLengthOperator(ScanShareableOperator):

    def __init__(self, property: MinLength):
        self.min_length_col = f"min_len{property.property_identifier()}"

        if property.where is None:
            aggregations = [f"min(length({property.column})) as {self.min_length_col}"]
        else:
            aggregations = [f"min(CASE WHEN ({property.where}) THEN length({property.column}) ELSE NULL END) as {self.min_length_col}"]

        super().__init__(property, aggregations)

    def get_property(self) -> MinLength:
        return self.property

    def extract_state(self, result_df: DataFrame) -> MinState:
        min_length = int(result_df[self.min_length_col][0])
        return MinState(self.property.property_identifier(), min_length)

    def get_metric(self, state: MinState) -> DoubleMetric:
        return metric_from_value(
            state.min_value, self.property.name, self.property.instance, self.property.entity
        )



class MinimumOperator(ScanShareableOperator):

    def __init__(self, property: Minimum):
        self.min_col = f"min{property.property_identifier()}"

        if property.where is None:
            aggregations = [f"min({property.column}) as {self.min_col}"]
        else:
            aggregations = [f"min(CASE WHEN ({property.where}) THEN {property.column}) ELSE NULL END) as {self.min_col}"]

        super().__init__(property, aggregations)

    def get_property(self) -> Minimum:
        return self.property

    def extract_state(self, result_df: DataFrame) -> MinState:
        min = float(result_df[self.min_col][0])
        return MinState(self.property.property_identifier(), min)

    def get_metric(self, state: MinState) -> DoubleMetric:
        return metric_from_value(
            state.min_value, self.property.name, self.property.instance, self.property.entity
        )


class MaximumOperator(ScanShareableOperator):

    def __init__(self, property: Maximum):
        self.max_col = f"max{property.property_identifier()}"

        if property.where is None:
            aggregations = [f"max({property.column}) as {self.max_col}"]
        else:
            aggregations = [f"max(CASE WHEN ({property.where}) THEN {property.column} ELSE NULL END) as {self.max_col}"]

        super().__init__(property, aggregations)

    def get_property(self) -> Maximum:
        return self.property

    def extract_state(self, result_df: DataFrame) -> MaxState:
        max = float(result_df[self.max_col][0])
        return MaxState(self.property.property_identifier(), max)

    def get_metric(self, state: MaxState) -> DoubleMetric:
        return metric_from_value(
            state.max_value, self.property.name, self.property.instance, self.property.entity
        )



class MeanOperator(ScanShareableOperator):
    def __init__(self, property: Mean):
        self.count_column = f"count{property.filter_identifier()}"
        self.total_column = f"total{property.property_identifier()}"

        if property.where is None:
            aggregations = [f"COUNT(*) as {self.count_column}",
                            f"SUM({property.column}) as {self.total_column}"]
        else:
            aggregations = [f"SUM(CASE WHEN ({property.where}) THEN 1 ELSE 0 END) as {self.count_column}",
                            f"SUM(CASE WHEN ({property.where}) THEN {property.column} ELSE NULL END) as {self.total_column}"]

        super().__init__(property, aggregations)

    def get_property(self) -> Mean:
        return self.property

    def extract_state(self, result_df: DataFrame) -> MeanState:
        total = float(result_df[self.total_column][0])
        count = int(result_df[self.count_column][0])
        return MeanState(self.property.property_identifier(), total, count)

    def get_metric(self, state: MeanState) -> DoubleMetric:
        return metric_from_value(
            state.total/state.count, self.property.name, self.property.instance, self.property.entity
        )

class SumOperator(ScanShareableOperator):
    def __init__(self, property: Sum):
        self.sum_column = f"total{property.property_identifier()}"

        if property.where is None:
            aggregations = [f"SUM({property.column}) as {self.sum_column}"]
        else:
            aggregations = [f"SUM(CASE WHEN ({property.where}) THEN {property.column} ELSE NULL END) as {self.sum_column}"]

        super().__init__(property, aggregations)

    def get_property(self) -> Sum:
        return self.property

    def extract_state(self, result_df: DataFrame) -> SumState:
        sum_value = float(result_df[self.sum_column][0])
        return SumState(self.property.property_identifier(), sum_value)

    def get_metric(self, state: SumState) -> DoubleMetric:
        return metric_from_value(
            state.sum_value, self.property.name, self.property.instance, self.property.entity
        )


class StandardDeviationOperator(ScanShareableOperator):
    def __init__(self, property: StandardDeviation):
        self.stddev_column = f"stddev_{property.property_identifier()}"

        if property.where is None:
            aggregations = [f"STDDEV_STATE({property.column}) as {self.stddev_column}"]
        else:
            aggregations = [f"STDDEV_STATE(CASE WHEN ({property.where}) THEN {property.column} ELSE NULL END) as {self.stddev_column}"]

        super().__init__(property, aggregations)

    def get_property(self) -> StandardDeviation:
        return self.property

    def extract_state(self, result_df: DataFrame) -> StandardDeviationState:
        stddev_json = str(result_df[self.stddev_column][0])
        stddev = json.loads(stddev_json)
        return StandardDeviationState(self.property.property_identifier(), n=float(stddev["count"]), avg=float(stddev["mean"]), m2=float(stddev["dsquared"]), stddev=float(stddev["stddev"]))

    def get_metric(self, state: StandardDeviationState) -> DoubleMetric:
        return metric_from_value(
            state.stddev, self.property.name, self.property.instance, self.property.entity
        )


class QuantileOperator(ScanShareableOperator):
    pass


class HistogramPropertyOperator(SQLOperator):
    pass

class ApproxDistinctOperator(SQLOperator):
    pass

class SQLOperatorFactory():
    property_operator_map = {
        Size : SizeOperator,
        HistogramProperty : HistogramPropertyOperator,
        Completeness : CompletenessOperator,
        Uniqueness : UniquenessOperator,
        Distinctness : DistinctnessOperator,
        UniqueValueRatio : UniqueValueRatioOperator,
        Compliance : ComplianceOperator,
        PatternMatch : PatternMatchOperator,
        Quantile : QuantileOperator,
        MaxLength : MaxLengthOperator,
        MinLength : MinLengthOperator,
        Minimum : MinimumOperator,
        Maximum : MaximumOperator,
        Mean : MeanOperator,
        Sum : SumOperator,
        StandardDeviation : StandardDeviationOperator,
        ApproxDistinctness : ApproxDistinctOperator
}

    @classmethod
    def create_operator(cls, property: Property) -> SQLOperator:
        PropertyType = type(property)

        if PropertyType in cls.property_operator_map:
            return cls.property_operator_map[PropertyType](property)
        else:
            raise UnsupportedPropertyException(f"Property '{PropertyType.__name__}' not supported by SQL Engine.")
