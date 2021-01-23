import hashlib
from abc import ABCMeta, abstractmethod
from typing import List, Optional, Sequence
import ctypes

from duckdq.core.metrics import DoubleMetric, SchemaMetric, Entity
from duckdq.core.preconditions import Precondition, is_numeric, is_string


class Property(metaclass=ABCMeta):
    def __init__(self, instance: str, entity: Entity, where: Optional[str] = None):
        self.name = self.__class__.__name__
        self.instance = instance
        self.entity = entity
        self.where = where

    def preconditions(self) -> List[Precondition]: #TODO: add preconditions
        return []

    def additional_preconditions(self) -> List[Precondition]:
        return []

    def description(self):
        return f"{self.__repr__()}_{self.__hash__()}"

    def __eq__(self, other):
        return (
            isinstance(other, Property)
            and self.name == self.name
            and self.instance == other.instance
            and self.entity == other.entity
            and self.where == other.where
        )

    def __hash__(self,):
        s = self.name + self.instance + self.entity.name + ('' if self.where is None else self.where)
        return int(hashlib.sha1(s.encode("utf-8")).hexdigest(), 16) % (10 ** 16)

    def filter_identifier(self,):
        if self.where is None:
            return ""
        else:
            return ctypes.c_size_t(hash(self.where)).value

    def property_identifier(self,):
        return ctypes.c_size_t(self.__hash__()).value

    def __repr__(self,):
        instance_summary = self.instance
        if len(self.instance) > 120:
            instance_summary = f"{self.instance[:40]} ... {self.instance[-40:]}"
        return f"{self.name}({instance_summary})"

    @abstractmethod
    def metric_type(self):
        pass


class DatasetProperty(Property):
    def __init__(self, instance: str, where: Optional[str] = None):
        super().__init__(instance, Entity.DATASET, where)

    def preconditions(self) -> List[Precondition]:
        return self.additional_preconditions() + super().preconditions()


class SingleColumnProperty(Property):
    def __init__(self, column: str, where: Optional[str] = None):
        super().__init__(column, Entity.COLUMN, where)
        self.column = column

    def preconditions(self) -> List[Precondition]:
        return self.additional_preconditions() + super().preconditions()


class TwoColumnProperty(Property):
    def __init__(self, columnA: str, columnB: str, where: Optional[str] = None):
        super().__init__(f"{columnA}_{columnB}", Entity.TWOCOLUMN, where)
        self.columnA = columnA
        self.columnB = columnB

    def preconditions(self) -> List[Precondition]:
        return self.additional_preconditions() + super().preconditions()

class MultiColumnProperty(Property):
    def __init__(self, columns: Sequence[str], where: Optional[str] = None):
        super().__init__(f"{'_'.join(columns)}", Entity.MULTICOLUMN, where)
        self.columns = columns

    def preconditions(self) -> List[Precondition]:
        return self.additional_preconditions() + super().preconditions()

class Schema(DatasetProperty):
    def __init__(self):
        super().__init__("")

    def metric_type(self):
        return SchemaMetric


class Size(DatasetProperty):
    def __init__(self, where: Optional[str] = None):
        super().__init__("*", where)

    def metric_type(self):
        return DoubleMetric


class HistogramProperty(SingleColumnProperty):
    def __init__(self, column: str, binningUDF, maxBins, where: Optional[str] = None):
        super().__init__(column, where)
        self.instance = f"{column}_{binningUDF}_{maxBins}"
        self.binningUDF = binningUDF
        self.maxBins = maxBins

    def metric_type(self):
        return DoubleMetric


class ApproxDistinctness(SingleColumnProperty):
    def metric_type(self):
        return DoubleMetric


class Completeness(SingleColumnProperty):
    def metric_type(self):
        return DoubleMetric


class Uniqueness(MultiColumnProperty):
    def metric_type(self):
        return DoubleMetric


class Distinctness(MultiColumnProperty):
    def metric_type(self):
        return DoubleMetric


class UniqueValueRatio(MultiColumnProperty):
    def metric_type(self):
        return DoubleMetric


class Compliance(DatasetProperty):
    def __init__(self, name: str, expression: str, where: Optional[str] = None):
        super().__init__(name, where)
        self.expression = expression

    def metric_type(self):
        return DoubleMetric


class PatternMatch(SingleColumnProperty):
    def __init__(self, column: str, pattern: str, where: Optional[str] = None):
        super().__init__(column, where)
        self.pattern = pattern
        self.instance = f"{column}_{pattern}"

    def metric_type(self):
        return DoubleMetric


class Entropy(SingleColumnProperty):
    def metric_type(self):
        return DoubleMetric


class Quantile(SingleColumnProperty):
    def __init__(self, column: str, quantile: str, where: Optional[str] = None):
        super().__init__(column, where)
        self.quantile = quantile
        self.instance = f"{column}_{quantile}"

    def metric_type(self):
        return DoubleMetric


class MaxLength(SingleColumnProperty):
    def metric_type(self):
        return DoubleMetric

    def additional_preconditions(self) -> List[Precondition]:
        return [is_string(self.column)]


class MinLength(SingleColumnProperty):
    def metric_type(self):
        return DoubleMetric

    def additional_preconditions(self) -> List[Precondition]:
        return [is_string(self.column)]


class Minimum(SingleColumnProperty):
    def metric_type(self):
        return DoubleMetric


class Maximum(SingleColumnProperty):
    def metric_type(self):
        return DoubleMetric


class Mean(SingleColumnProperty):
    def metric_type(self):
        return DoubleMetric


class Sum(SingleColumnProperty):
    def metric_type(self):
        return DoubleMetric


class StandardDeviation(SingleColumnProperty):
    def metric_type(self):
        return DoubleMetric


class Correlation(TwoColumnProperty):
    def metric_type(self):
        return DoubleMetric


class KllSketch(SingleColumnProperty):
    def __init__(self, column: str, kll_parameters: str, where: Optional[str] = None):
        super().__init__(column, where)
        self.kll_parameters = kll_parameters
        self.instance = f"{column}_{kll_parameters}"

    def metric_type(self):
        return DoubleMetric
