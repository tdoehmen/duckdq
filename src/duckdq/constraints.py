# Apache Software License 2.0
#
# Modifications copyright (C) 2021, Till Döhmen, Fraunhofer FIT
# Copyright (c) 2019, Miguel Cabrera
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from abc import ABC, abstractmethod
from dataclasses import dataclass, replace
from enum import Enum
from typing import Callable, Optional, Pattern, Sequence, Union, Mapping, Generic, TypeVar, Dict
from tryingsnake import Success

from duckdq.core.metrics import Metric
from duckdq.core.properties import (
    Completeness,
    Compliance,
    Maximum,
    Mean,
    Minimum,
    PatternMatch,
    Quantile,
    Size,
    StandardDeviation,
    Sum,
    Uniqueness, Property, Schema, Distinctness, MinLength, MaxLength, ApproxDistinctness,
)

class ConstraintStatus(Enum):
    SUCCESS = 0
    FAILURE = 1


class Constraint(ABC):
    # Common trait for all data quality constraints

    @abstractmethod
    def evaluate(
        self, analysis_result: Mapping[Property, Metric]
    ) -> "ConstraintResult":
        pass

@dataclass
class ConstraintResult:
    constraint: Constraint
    status: ConstraintStatus
    message: Optional[str] = None
    metric: Optional[Metric] = None


_MISSING_ANALYSIS_MSG = "Missing Analysis, can't run the constraint!"
_ASSERTION_EXCEPTION_MSG = "Can't execute the assertion"


class ConstraintAssertionException(Exception):
    pass

V = TypeVar("V")

class AnalysisBasedConstraint(Constraint, Generic[V]):
    """
    Common functionality for all analysis based constraints that
    provides unified way to access AnalyzerContext and metrics stored in it.

    Runs the analysis and get the value of the metric returned by the analysis,
    picks the numeric value that will be used in the assertion function with
    metric picker runs the assertion.
    """

    # TODO: Check if implementing value pickler makes sense
    def __init__(
        self,
        analyzer: Property,
        assertion: Callable[[V], bool],
        hint: Optional[str] = None,
    ):
        """
        Parameters
        ----------

        analyzer:
            Analyzer to be run on the data frame
        assertion:   Assertion callable
        value_picker: (NOT IMPLEMENTED)
            Optional function to pick the interested part of the
            metric value that the assertion will be running on.
            Absence of such function means the metric value would be
            used in the assertion as it is.
        hint:
             A hint to provide additional context why a constraint could have failed
        """
        self.analyzer = analyzer
        self._assertion = assertion  # type: ignore
        self._hint = hint

    def evaluate(self, analysis_result: Mapping[Property, Metric]):
        metric: Optional[Metric] = analysis_result.get(self.analyzer, None)

        if metric is None:
            return ConstraintResult(
                self, ConstraintStatus.FAILURE, _MISSING_ANALYSIS_MSG, metric
            )

        return self._pick_value_and_assert(metric)

    def _pick_value_and_assert(self, metric: Metric) -> ConstraintResult:
        metric_value = metric.value
        hint = self._hint or ""
        if isinstance(metric_value, Success):
            try:
                # TODO: run_picker_on_metric, not sure if needed
                assert_on = metric_value.get()
                # run assertion
                assertion_ok = self._run_assertion(assert_on)
                if assertion_ok:
                    return ConstraintResult(
                        self, ConstraintStatus.SUCCESS, metric=metric
                    )
                else:
                    msg = (
                        f"Value {assert_on} does not meet the constraint requirement. "
                        f"{hint}"
                    )
                    return ConstraintResult(self, ConstraintStatus.FAILURE, msg, metric)
            except ConstraintAssertionException as ex:
                return ConstraintResult(
                    self,
                    ConstraintStatus.FAILURE,
                    f"{_ASSERTION_EXCEPTION_MSG}: {str(ex)}",
                    metric,
                )
        else:  # then is a Failure
            e = metric_value.failed().get()
            return ConstraintResult(self, ConstraintStatus.FAILURE, str(e), metric)

    def _run_assertion(self, assert_on):
        try:
            assertion_result = self._assertion(assert_on)  # type: ignore
        except Exception as e:
            raise ConstraintAssertionException(e) from e
        return assertion_result

class ConstraintDecorator(Constraint):
    def __init__(self, inner: Constraint):
        self._inner = inner

    @property
    def inner(self) -> Constraint:
        if isinstance(self._inner, ConstraintDecorator):
            return self._inner.inner
        else:
            return self._inner

    def evaluate(
        self, analysis_result: Mapping[Property, Metric]
    ) -> "ConstraintResult":

        return replace(self._inner.evaluate(analysis_result), constraint=self)


@dataclass(eq=True)
class NamedConstraint(ConstraintDecorator):
    constraint: Constraint
    name: str

    def __init__(self, constraint: Constraint, name: str):
        super().__init__(constraint)
        self.name = name
        self.constraint = constraint

    def __str__(self):
        return self.name

    def __hash__(self,):
        return (
            hash(self.constraint) ^ hash(self.name)
        )

    def __repr__(self):
        return self.name

# A lot of mypy ignores because mypy is not able to understand that the
# Analyzers are specialization of Analyzer[K, S, V]
def schema_constraint(
    assertion: Callable[[Dict[str,str]], bool],
    hint: Optional[str] = None,
) -> Constraint:

    if not callable(assertion):
        raise ValueError("assertion is not a callable")

    schema = Schema()
    constraint = AnalysisBasedConstraint[Dict[str,str]](
        schema, assertion, hint=hint  # type: ignore[arg-type]
    )

    return NamedConstraint(constraint, f"SchemaConstraint({schema})")

def size_constraint(
    assertion: Callable[[int], bool],
    where: Optional[str] = None,
    hint: Optional[str] = None,
) -> Constraint:

    if not callable(assertion):
        raise ValueError("assertion is not a callable")

    size = Size(where)
    constraint = AnalysisBasedConstraint[int](
        size, assertion, hint=hint  # type: ignore[arg-type]
    )

    return NamedConstraint(constraint, f"SizeConstraint({size})")


def min_constraint(
    column: str,
    assertion: Callable[[float], bool],
    where: Optional[str] = None,
    hint: Optional[str] = None,
) -> Constraint:

    minimum = Minimum(column, where)
    constraint = AnalysisBasedConstraint[float](
        minimum, assertion, hint=hint  # type: ignore[arg-type]
    )

    return NamedConstraint(constraint, f"MinimumConstraint({minimum})")


def max_constraint(
    column: str,
    assertion: Callable[[float], bool],
    where: Optional[str] = None,
    hint: Optional[str] = None,
) -> Constraint:

    maximum = Maximum(column, where)
    constraint = AnalysisBasedConstraint[float](
        maximum, assertion, hint=hint  # type: ignore[arg-type]
    )

    return NamedConstraint(constraint, f"MaximumConstraint({maximum})")

def min_length_constraint(
    column: str,
    assertion: Callable[[float], bool],
    where: Optional[str] = None,
    hint: Optional[str] = None,
) -> Constraint:

    minimum = MinLength(column, where)
    constraint = AnalysisBasedConstraint[float](
        minimum, assertion, hint=hint  # type: ignore[arg-type]
    )

    return NamedConstraint(constraint, f"MinLengthConstraint({minimum})")


def max_length_constraint(
    column: str,
    assertion: Callable[[float], bool],
    where: Optional[str] = None,
    hint: Optional[str] = None,
) -> Constraint:

    maximum = MaxLength(column, where)
    constraint = AnalysisBasedConstraint[float](
        maximum, assertion, hint=hint  # type: ignore[arg-type]
    )

    return NamedConstraint(constraint, f"MaxLengthConstraint({maximum})")


def completeness_constraint(
    column: str,
    assertion: Callable[[float], bool],
    where: Optional[str] = None,
    hint: Optional[str] = None,
) -> Constraint:

    completeness = Completeness(column, where)
    constraint = AnalysisBasedConstraint[float](
        completeness, assertion, hint=hint  # type: ignore[arg-type]
    )

    return NamedConstraint(constraint, f"CompletenessConstraint({completeness})")


def mean_constraint(
    column: str,
    assertion: Callable[[float], bool],
    where: Optional[str] = None,
    hint: Optional[str] = None,
) -> Constraint:

    mean = Mean(column, where)
    constraint = AnalysisBasedConstraint[float](
        mean, assertion, hint=hint  # type: ignore[arg-type]
    )

    return NamedConstraint(constraint, f"MeanConstraint({mean})")


def sum_constraint(
    column: str,
    assertion: Callable[[float], bool],
    where: Optional[str] = None,
    hint: Optional[str] = None,
) -> Constraint:

    sum_ = Sum(column, where)
    constraint = AnalysisBasedConstraint[float](
        sum_, assertion, hint=hint  # type: ignore[arg-type]
    )

    return NamedConstraint(constraint, f"SumConstraint({sum})")


def standard_deviation_constraint(
    column: str,
    assertion: Callable[[float], bool],
    where: Optional[str] = None,
    hint: Optional[str] = None,
) -> Constraint:

    std = StandardDeviation(column, where)
    constraint = AnalysisBasedConstraint[float](
        std, assertion, hint=hint  # type: ignore[arg-type]
    )

    return NamedConstraint(constraint, f"StandardDeviationConstraint({std})")


def quantile_constraint(
    column: str,
    quantile: float,
    assertion: Callable[[float], bool],
    where: Optional[str] = None,
    hint: Optional[str] = None,
) -> Constraint:
    """
    Runs quantile analysis on the given column and executes the assertion

    column:
        Column to run the assertion on
    quantile:
        Which quantile to assert on
    assertion
        Callable that receives a float input parameter (the computed quantile)
        and returns a boolean
    hint:
        A hint to provide additional context why a constraint could have failed
    """
    quant = Quantile(column, quantile, where)
    constraint = AnalysisBasedConstraint[float](
        quant, assertion, hint=hint  # type: ignore[arg-type]
    )

    return NamedConstraint(constraint, f"QuantileConstraint({quant})")


def approx_distinctness_constraint(
        column: str,
        quantile: float,
        assertion: Callable[[float], bool],
        where: Optional[str] = None,
        hint: Optional[str] = None,
) -> Constraint:
    """
    Runs approx distinctness analysis on the given column and executes the assertion

    column:
        Column to run the assertion on
    quantile:
        Which quantile to assert on
    assertion
        Callable that receives a float input parameter (the computed quantile)
        and returns a boolean
    hint:
        A hint to provide additional context why a constraint could have failed
    """
    approx_distinct = ApproxDistinctness(column, where)
    constraint = AnalysisBasedConstraint[float](
        approx_distinct, assertion, hint=hint  # type: ignore[arg-type]
    )

    return NamedConstraint(constraint, f"ApproxDistinctness({approx_distinct})")



def compliance_constraint(
    name: str,
    column: str,
    assertion: Callable[[float], bool],
    where: Optional[str] = None,
    hint: Optional[str] = None,
) -> Constraint:
    """
    Runs given the expression on the given column(s) and executes the assertion

    Parameters:
    ---------
    name:
        A name that summarizes the check being made. This name is being used to name the
        metrics for the analysis being done.
    column:
        The column expression to be evaluated.
    assertion:
        Callable that receives a float input parameter and returns a boolean
    where:
        Additional filter to apply before the analyzer is run.
    hint:
         A hint to provide additional context why a constraint could have failed

    """
    compliance = Compliance(name, column, where)
    constraint = AnalysisBasedConstraint[float](
        compliance, assertion, hint=hint  # type: ignore[arg-type]
    )

    return NamedConstraint(constraint, f"ComplianceConstraint({compliance})")


def uniqueness_constraint(
    columns: Sequence[str],
    assertion: Callable[[float], bool],
    where: Optional[str] = None,
    hint: Optional[str] = None,
) -> Constraint:
    """
    Runs Uniqueness analysis on the given columns and executes the assertion
    columns.

    Parameters:
    ----------

    columns:
        Columns to run the assertion on.
    assertion:
        Callable that receives a float input parameter and returns a boolean
    where:
        Additional filter to apply before the analyzer is run.
    hint:
         A hint to provide additional context why a constraint could have failed

    """

    uniqueness = Uniqueness(columns, where)
    constraint = AnalysisBasedConstraint[float](
        uniqueness, assertion, hint=hint  # type: ignore[arg-type]
    )

    return NamedConstraint(constraint, f"UniquenessConstraint({uniqueness})")

def distinctness_constraint(
    columns: Sequence[str],
    assertion: Callable[[float], bool],
    where: Optional[str] = None,
    hint: Optional[str] = None,
) -> Constraint:
    """
    Runs Distinctness analysis on the given columns and executes the assertion
    columns.

    Parameters:
    ----------

    columns:
        Columns to run the assertion on.
    assertion:
        Callable that receives a float input parameter and returns a boolean
    where:
        Additional filter to apply before the analyzer is run.
    hint:
         A hint to provide additional context why a constraint could have failed

    """

    distinctness = Distinctness(columns, where)
    constraint = AnalysisBasedConstraint[float](
        distinctness, assertion, hint=hint  # type: ignore[arg-type]
    )

    return NamedConstraint(constraint, f"DistinctnessConstraint({distinctness})")

def pattern_match_constraint(
    column: str,
    pattern: Union[str, Pattern],
    assertion: Callable[[float], bool],
    where: Optional[str] = None,
    name: Optional[str] = None,
    hint: Optional[str] = None,
) -> Constraint:
    """
    Runs given regex compliance analysis on the given column(s) and executes the
    assertion.

    Parameters
    ----------
    column:
          The column to run the assertion on
    pattern:
        The regex pattern to check compliance for (either string or pattern instance)
    where:
        Additional filter to apply before the analyzer is run.
    name:
        A name that summarizes the check being made. This name is being used
        to name the metrics for the analysis being done.
    hint:
        A hint to provide additional context why a constraint could have failed
    """

    pattern_match = PatternMatch(column, pattern, where)

    constraint = AnalysisBasedConstraint[float](
        pattern_match, assertion, hint=hint  # type: ignore[arg-type]
    )

    name = (
        f"PatternMatchConstraint({name})"
        if name
        else f"PatternMatchConstraint({column}, {pattern})"
    )

    return NamedConstraint(constraint, name)
