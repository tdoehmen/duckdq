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
#

from dataclasses import dataclass, field
from enum import Enum, IntEnum
from typing import (
    Any,
    Callable,
    List,
    Optional,
    Pattern,
    Sequence,
    Set,
    Tuple,
    Union,
    cast, Dict,
)

import duckdq.utils.patterns as patterns
import numpy as np
from duckdq.utils.analysis_runner import AnalyzerContext
from duckdq.constraints import (
    AnalysisBasedConstraint,
    Constraint,
    ConstraintDecorator,
    ConstraintResult,
    completeness_constraint,
    compliance_constraint,
    max_constraint,
    mean_constraint,
    min_constraint,
    pattern_match_constraint,
    quantile_constraint,
    size_constraint,
    standard_deviation_constraint,
    sum_constraint,
    uniqueness_constraint, schema_constraint, distinctness_constraint, max_length_constraint, min_length_constraint,
    approx_distinctness_constraint,
)
from duckdq.constraints import ConstraintStatus
from duckdq.core.properties import Property


class CheckLevel(Enum):
    WARNING = 0
    EXCEPTION = 1


class CheckStatus(IntEnum):
    SUCCESS = 0
    WARNING = 1
    ERROR = 2


@dataclass(frozen=True, eq=True)
class CheckResult:
    check: Any
    status: CheckStatus
    constraint_results: Sequence[ConstraintResult] = field(default_factory=tuple)




def is_one(value: Union[float, int]) -> bool:
    return value == 1

def is_approx_one(value: Union[float, int]) -> bool:
    return (value > 0.99) and (value <= 1)

@dataclass(frozen=True, eq=True)
class Check:
    level: CheckLevel
    description: str
    constraints: Tuple[Constraint, ...] = field(default_factory=tuple)

    def add_constraint(self, constraint: Constraint) -> "Check":
        """
        Returns a new Check object with the given constraint added to the
        constraints list.

        Parameters
        -------------

        constraint:
             New constraint to be added
        """
        return Check(self.level, self.description, self.constraints + (constraint,))

    def _add_filterable_constraint(
        self, creation_func: Callable[[Optional[str]], Constraint]
    ) -> "CheckWithLastConstraintFilterable":
        """
        Adds a constraint that can subsequently be replaced
        with a filtered version.
        """

        constraint_without_filtering = creation_func(None)
        return CheckWithLastConstraintFilterable(
            self.level,
            self.description,
            self.constraints + (constraint_without_filtering,),
            creation_func,
        )

    def required_analyzers(self) -> Set[Property]:

        rc = (
            c.inner if isinstance(c, ConstraintDecorator) else c
            for c in self.constraints
        )  # map
        anbc: List[AnalysisBasedConstraint] = cast(
            List[AnalysisBasedConstraint],
            list(filter(lambda c: isinstance(c, AnalysisBasedConstraint), rc)),
        )  # collect

        analyzers = {c.analyzer for c in anbc}  # map

        return analyzers

    def has_schema(
        self, assertion: Callable[[Dict[str,str]], bool], hint: Optional[str] = None
    ) -> "CheckWithLastConstraintFilterable":
        """
        Creates a constraint that calculates the data frame size and runs the assertion
        on it.

        Parameters
        ----------

        assertion:
               A callable that receives a dict[str,str] input parameter and returns a boolean.
               The callable will receive the schema as dict (key: column name, value: column type)
               and return a boolean based on whether it satisfies a condition, e.g.
               ``lambda s: "columnA" in s``.
        hint:
               A hint to provide additional context why a constraint could have failed
        """

        return self.add_constraint(schema_constraint(assertion, hint))

    def has_size(
        self, assertion: Callable[[int], bool], hint: Optional[str] = None
    ) -> "CheckWithLastConstraintFilterable":
        """
        Creates a constraint that calculates the data frame size and runs the assertion
        on it.

        Parameters
        ----------

        assertion:
               A callable that receives a long input parameter and returns a boolean.
               The callable will receive the value of the size (number of rows)
               and return a boolean based on whether it satisfies a condition, e.g.
               ``lambda sz: sz > 5``.
        hint:
               A hint to provide additional context why a constraint could have failed
        """

        return self._add_filterable_constraint(
            lambda filter_: size_constraint(assertion, filter_, hint)
        )

    def has_min(
        self,
        column: str,
        assertion: Callable[[float], bool],
        hint: Optional[str] = None,
    ) -> "CheckWithLastConstraintFilterable":
        """
        Creates a constraint that asserts on the minimum of the column

        Parameters
        ----------

        column:
                Column to run the assertion on.

        assertion:
                A callable that receives a float and returns a boolean
        hint:
                A hint to provide additional context why a constraint could have failed

        """

        return self._add_filterable_constraint(
            lambda filter_: min_constraint(column, assertion, filter_, hint)
        )

    def has_max(
        self,
        column: str,
        assertion: Callable[[float], bool],
        hint: Optional[str] = None,
    ) -> "CheckWithLastConstraintFilterable":
        """
        Creates a constraint that asserts on the maximum of the column

        Parameters
        ----------

        column:
                Column to run the assertion on.
        assertion:
                A callable that receives a float and returns a boolean
        hint:
                A hint to provide additional context why a constraint could have failed

        """

        return self._add_filterable_constraint(
            lambda filter_: max_constraint(column, assertion, filter_, hint)
        )

    def has_min_length(
        self,
        column: str,
        assertion: Callable[[float], bool],
        hint: Optional[str] = None,
    ) -> "CheckWithLastConstraintFilterable":
        """
        Creates a constraint that asserts on the minimum length of the column

        Parameters
        ----------

        column:
                Column to run the assertion on.

        assertion:
                A callable that receives a float and returns a boolean
        hint:
                A hint to provide additional context why a constraint could have failed

        """

        return self._add_filterable_constraint(
            lambda filter_: min_length_constraint(column, assertion, filter_, hint)
        )

    def has_max_length(
        self,
        column: str,
        assertion: Callable[[float], bool],
        hint: Optional[str] = None,
    ) -> "CheckWithLastConstraintFilterable":
        """
        Creates a constraint that asserts on the maximum length of the column

        Parameters
        ----------

        column:
                Column to run the assertion on.
        assertion:
                A callable that receives a float and returns a boolean
        hint:
                A hint to provide additional context why a constraint could have failed

        """

        return self._add_filterable_constraint(
            lambda filter_: max_length_constraint(column, assertion, filter_, hint)
        )

    def is_complete(
        self, column: str, hint: Optional[str] = None,
    ) -> "CheckWithLastConstraintFilterable":
        """
        Creates a constraint that asserts on a column completion.

        Parameters
        ----------

        column:
                Column to run the assertion on.

        """
        return self._add_filterable_constraint(
            lambda filter_: completeness_constraint(column, is_one, filter_, hint)
        )

    def has_completeness(
        self,
        column: str,
        assertion: Callable[[float], bool],
        hint: Optional[str] = None,
    ) -> "CheckWithLastConstraintFilterable":
        """
        Creates a constraint that asserts on a column completion

        Parameters
        ----------

        column:
                Column to run the assertion on.
        assertion:
                A callable that receives a float and returns a boolean
        hint:
                A hint to provide additional context why a constraint could have failed

        """
        return self._add_filterable_constraint(
            lambda filter_: completeness_constraint(column, assertion, filter_, hint)
        )

    def has_mean(
        self,
        column: str,
        assertion: Callable[[float], bool],
        hint: Optional[str] = None,
    ) -> "CheckWithLastConstraintFilterable":
        """

        Creates a constraint that asserts on the mean of the column.

        Parameters
        ----------

        column:
                Column to run the assertion on.
        assertion:
                A callable that receives a float and returns a boolean
        hint:
                A hint to provide additional context why a constraint could have failed

        """

        return self._add_filterable_constraint(
            lambda filter_: mean_constraint(column, assertion, filter_, hint)
        )

    def has_standard_deviation(
        self,
        column: str,
        assertion: Callable[[float], bool],
        hint: Optional[str] = None,
    ) -> "CheckWithLastConstraintFilterable":
        """

        Creates a constraint that asserts on the standard deviation of the column.
        Note that unlike pandas this calculate the population variance
        i.e. degree of freedom (ddof=0). NaNs are ignored when performing the
        calculation.

        Parameters
        ----------

        column:
                Column to run the assertion on.
        assertion:
                A callable that receives a float and returns a boolean
        hint:
                A hint to provide additional context why a constraint could have failed

        """
        return self._add_filterable_constraint(
            lambda filter_: standard_deviation_constraint(
                column, assertion, filter_, hint
            )
        )

    def has_sum(
        self,
        column: str,
        assertion: Callable[[float], bool],
        hint: Optional[str] = None,
    ) -> "CheckWithLastConstraintFilterable":
        """

        Creates a constraint that asserts on the sum of the column.


        Parameters
        ----------

        column:
                Column to run the assertion on.
        assertion:
                A callable that receives a float and returns a boolean
        hint:
                A hint to provide additional context why a constraint could have failed

        """
        return self._add_filterable_constraint(
            lambda filter_: sum_constraint(column, assertion, filter_, hint)
        )

    def has_approx_quantile(
            self,
            column: str,
            q: float,
            assertion: Callable[[float], bool],
            hint: Optional[str] = None,
    ) -> "CheckWithLastConstraintFilterable":
        """

        Creates a constraint that asserts on the quantile of the column.

        Parameters
        ----------

        column:
            Column to run the assertion on.
        q:
            The q-th quantile to calculate which must be between 0 and 1 inclusive.
        assertion:
            A callable that receives a float and returns a boolean
        hint:
            A hint to provide additional context why a constraint could have failed

        """
        return self._add_filterable_constraint(
            lambda filter_: quantile_constraint(column, q, assertion, filter_, hint)
        )

    def has_approx_distinctness(
            self,
            column: str,
            assertion: Callable[[float], bool],
            hint: Optional[str] = None,
    ) -> "CheckWithLastConstraintFilterable":
        """

        Creates a constraint that asserts on the approximate distinctness of the column.

        Parameters
        ----------

        column:
            Column to run the assertion on.
        assertion:
            A callable that receives a float and returns a boolean
        hint:
            A hint to provide additional context why a constraint could have failed

        """
        return self._add_filterable_constraint(
            lambda filter_: approx_distinctness_constraint(column, assertion, filter_, hint)
        )

    def is_approx_distinct(
            self,
            column: str,
            assertion: Callable[[float], bool] = is_approx_one,
            hint: Optional[str] = None,
    ) -> "CheckWithLastConstraintFilterable":
        """
        Creates a constraint that asserts that a column is distinct

        Parameters
        ----------

        column:
            Column to run the assertion on
        assertion:
            Callable that receives a float input parameter and returns a boolean
        hint:
            A hint to provide additional context why a constraint could have failed

        """
        return self._add_filterable_constraint(
            lambda filter_: approx_distinctness_constraint(column, assertion, filter_, hint)
        )

    def satisfies(
        self,
        column_condition: str,
        constraint_name: str,
        assertion: Callable[[float], bool] = is_one,
        hint: Optional[str] = None,
    ) -> "CheckWithLastConstraintFilterable":
        """

        Creates a constraint that evaluates on the column_condition
        and executes the assertion.
        This is useful for complex or custom checks that are better described
        using a valid expression.

        Parameters
        -----------

        column_condition:
            The column expression to be evaluated. If using a Pandas data-frame
            this expression is evaluated with ``pandas.eval``.
        constraint_name:
            A name that summarizes the check being made. This name is being used to name
            the metrics for the analysis being done.
        assertion:
            Callable that receives a float input parameter and returns a boolean.
        hint:
            A hint to provide additional context why a constraint could have failed

        """

        return self._add_filterable_constraint(
            lambda filter_: compliance_constraint(
                constraint_name, column_condition, assertion, filter_, hint
            )
        )

    def is_non_negative(
        self,
        column: str,
        assertion: Callable[[float], bool] = is_one,
        hint: Optional[str] = None,
    ) -> "CheckWithLastConstraintFilterable":
        """
        Creates a constraint that asserts that a column contains no negative values

        Parameters
        ----------

        column:
            Column to run the assertion on
        assertion:
            Callable that receives a float input parameter and returns a boolean
        hint:
            A hint to provide additional context why a constraint could have failed

        """
        # coalescing column to not count NULL values as non-compliant
        return self.satisfies(
            f"{column} >= 0 OR {column} IS NULL",
            f"{column} is non-negative",
            assertion,
            hint=hint,
        )

    def is_positive(
        self,
        column: str,
        assertion: Callable[[float], bool] = is_one,
        hint: Optional[str] = None,
    ) -> "CheckWithLastConstraintFilterable":
        """
        Creates a constraint that asserts that a column contains positive values.

        Parameters
        ----------

        column:
            Column to run the assertion on
        assertion:
            Callable that receives a float input parameter and returns a boolean
        hint:
            A hint to provide additional context why a constraint could have failed

        """
        # coalescing column to not count NULL values as non-compliant
        return self.satisfies(
            f"{column} > 0 OR {column} IS NULL",
            f"{column} is positive",
            assertion,
            hint=hint,
        )

    def is_contained_in(
        self,
        column: str,
        allowed_values: Sequence[Union[str, int]],
        assertion: Callable[[float], bool] = is_one,
        hint: Optional[str] = None,
    ) -> "CheckWithLastConstraintFilterable":
        """
        Asserts that every non-null value in a column is contained in a set of
        predefined values. Note that this only works on a set of string sequences.

        Parameters
        ----------

        column:
            Column to run the assertion on
        allowed_values:
            Allowed values for the column
        assertion:
            Callable that receives a float input parameter and returns a boolean
        hint:
            A hint to provide additional context why a constraint could have failed

        """

        allowed_values = list(allowed_values)

        if not allowed_values:
            raise ValueError("Empty list of allowed values used")

        is_numeric_sequence = all(
            isinstance(value, (int, np.integer, float, np.float)) for value in allowed_values
        )

        is_string_sequence = all(
            isinstance(value, (str)) for value in allowed_values
        )

        if is_numeric_sequence:
            predicate = f"{column} IS NULL or {column} IN ({', '.join([str(v) for v in allowed_values])})"
        elif is_string_sequence:
            allowed_values = [f"'{value}'" for value in allowed_values]
            predicate = f"{column} IS NULL or {column} IN ({', '.join(allowed_values)})"
        else:
            raise ValueError(
                "The type of allowed values should be string or integer but got"
                f" '{type(allowed_values[0])}'"
            )

        return self.satisfies(
            predicate, f"{column} contained in {allowed_values}", assertion, hint
        )

    def is_contained_in_range(
        self,
        column: str,
        lower_bound: float,
        upper_bound: float,
        include_lower_bound: bool = True,
        include_upper_bound: bool = True,
        hint: Optional[str] = None,
    ) -> "CheckWithLastConstraintFilterable":
        """
        Asserts that the non-null values in a numeric column fall into the
        predefined interval

        Parameters
        ----------

        column:
            Column to run the assertion on
        lower_bound:
            lower bound of the interval
        upper_bound:
            upper bound of the interval
        include_lower_bound:
            is a value equal to the lower bound allowed?
        include_upper_bound:
             is a value equal to the upper bound allowed?
        hint:
            A hint to provide additional context why a constraint could have failed

        """

        left_operand = ">=" if include_lower_bound else ">"
        right_operand = "<=" if include_upper_bound else "<"

        predicate = (
            f"{column} IS NULL OR "
            f"({column} {left_operand} {lower_bound} "
            f" AND {column} {right_operand} {upper_bound})"
        )
        return self.satisfies(
            predicate, f"{column} between {lower_bound} (include: {include_lower_bound}) and {upper_bound} (include: {include_upper_bound})", hint=hint
        )

    def is_unique(
        self, column: str, hint: Optional[str] = None
    ) -> "CheckWithLastConstraintFilterable":
        """
        Creates a constraint that asserts on a column uniqueness.

        Parameters
        ----------

        column:
            Column to run the assertion on
        hint:
            A hint to provide additional context why a constraint could have failed

        """

        return self._add_filterable_constraint(
            lambda filter_: uniqueness_constraint([column], is_one, filter_, hint)
        )

    def has_uniqueness(
        self,
        columns: Union[Sequence[str], str],
        assertion: Callable[[float], bool],
        hint: Optional[str] = None,
    ):
        """
        Creates a constraint that asserts on uniqueness in a single or combined
        set of key columns.


        Parameters
        ----------
        columns:
            Column or columns to run the assertion on
        assertion:
            Callable that receives a double input parameter and returns a boolean.
            The input is the fraction of unique values in columns.
        hint:
            A hint to provide additional context why a constraint could have failed
        """

        if isinstance(columns, str):
            columns = [columns]

        return self._add_filterable_constraint(
            lambda filter_: uniqueness_constraint(
                columns, assertion, filter_, hint=hint
            )
        )

    def is_distinct(
        self, column: str, hint: Optional[str] = None
    ) -> "CheckWithLastConstraintFilterable":
        """
        Creates a constraint that asserts on a column distinctness.

        Parameters
        ----------

        column:
            Column to run the assertion on
        hint:
            A hint to provide additional context why a constraint could have failed

        """

        return self._add_filterable_constraint(
            lambda filter_: distinctness_constraint([column], is_one, filter_, hint)
        )

    def has_distinctness(
        self,
        columns: Union[Sequence[str], str],
        assertion: Callable[[float], bool],
        hint: Optional[str] = None,
    ):
        """
        Creates a constraint that asserts on distinctness in a single or combined
        set of key columns.


        Parameters
        ----------
        columns:
            Column or columns to run the assertion on
        assertion:
            Callable that receives a double input parameter and returns a boolean.
            The input is the fraction of unique values in columns.
        hint:
            A hint to provide additional context why a constraint could have failed
        """

        if isinstance(columns, str):
            columns = [columns]

        return self._add_filterable_constraint(
            lambda filter_: distinctness_constraint(
                columns, assertion, filter_, hint=hint
            )
        )

    def has_pattern(
        self,
        column: str,
        pattern: Union[str, Pattern],
        assertion: Callable[[float], bool] = is_one,
        name: Optional[str] = None,
        hint: Optional[str] = None,
    ):
        """
        Checks for pattern compliance. Given a column name and a regular
        expression, defines a Check on the average compliance of the
        column's values to the regular expression.


        Parameters
        ----------
        column:
            Name of the column that should be checked.
        pattern:
            The columns values will be checked for a match against this pattern.
        assertion:
            Callable that receives a double input parameter and returns a boolean.
            The input is the fraction of unique values in columns.
        hint:
            A hint to provide additional context why a constraint could have failed

        """
        return self._add_filterable_constraint(
            lambda filter_: pattern_match_constraint(
                column, pattern, assertion, filter_, name=name, hint=hint
            )
        )

    def contains_credit_card_number(
        self,
        column: str,
        assertion: Callable[[float], bool] = is_one,
        hint: Optional[str] = None,
    ):
        """
        Check to run against the compliance of a column against a credit card pattern.

        Parameters
        ----------
        column:
            Name of the column that should be checked.
        assertion:
            Callable that receives a double input parameter and returns a boolean.
            The input is the fraction of unique values in columns.
        hint:
            A hint to provide additional context why a constraint could have failed
        """
        return self.has_pattern(
            column,
            patterns.CREDITCARD,
            assertion=assertion,
            name=f"containsCreditCardNumber({column})",
            hint=hint,
        )

    def contains_email(
        self,
        column: str,
        assertion: Callable[[float], bool] = is_one,
        hint: Optional[str] = None,
    ):
        """
        Check to run against the compliance of a column against a against an
        e-mail pattern.

        Parameters
        ----------
        column:
            Name of the column that should be checked.
        assertion:
            Callable that receives a double input parameter and returns a boolean.
            The input is the fraction of unique values in columns.
        hint:
            A hint to provide additional context why a constraint could have failed
        """
        return self.has_pattern(
            column,
            patterns.EMAIL,
            assertion=assertion,
            name=f"containsEmail({column})",
            hint=hint,
        )

    def contains_url(
        self,
        column: str,
        assertion: Callable[[float], bool] = is_one,
        hint: Optional[str] = None,
    ):
        """
        Check to run against the compliance of a column against a against an
        URL pattern.

        Parameters
        ----------
        column:
            Name of the column that should be checked.
        assertion:
            Callable that receives a double input parameter and returns a boolean.
            The input is the fraction of unique values in columns.
        hint:
            A hint to provide additional context why a constraint could have failed
        """
        return self.has_pattern(
            column,
            patterns.URL,
            assertion=assertion,
            name=f"containsURL({column})",
            hint=hint,
        )

    def evaluate(self, context: AnalyzerContext) -> CheckResult:
        """
        Evaluate this check on computed metrics

        Parameters
        ----------

        context:
            result of the metrics computation

        """
        constraint_results = [c.evaluate(context.metric_map) for c in self.constraints]
        any_failures: bool = any(
            (c.status == ConstraintStatus.FAILURE for c in constraint_results)
        )

        check_status = CheckStatus.SUCCESS

        if any_failures and self.level == CheckLevel.EXCEPTION:
            check_status = CheckStatus.ERROR
        elif any_failures and self.level == CheckLevel.WARNING:
            check_status = CheckStatus.WARNING

        return CheckResult(self, check_status, constraint_results)


class CheckWithLastConstraintFilterable(Check):
    def __init__(
        self,
        level: CheckLevel,
        description: str,
        constraints: Tuple[Constraint, ...],
        create_replacement: Callable[[Optional[str]], Constraint],
    ):
        super().__init__(level, description, constraints)
        self.create_replacement = create_replacement

    def where(self, query: Optional[str]) -> Check:
        """
        Defines a filter to apply before evaluating the previous constraint

        Parameters
        -----------
        filter:
            A Pandas `query` sring to evaluate.

        Returns
        --------
        A filtered Check

        """
        adjusted_constraints = self.constraints[:-1] + (self.create_replacement(query),)
        return Check(self.level, self.description, adjusted_constraints)

    @classmethod
    def apply(
        cls,
        level: CheckLevel,
        description: str,
        constraints: Tuple[Constraint, ...],
        create_replacement: Callable[[Optional[str]], Constraint],
    ) -> "CheckWithLastConstraintFilterable":

        return CheckWithLastConstraintFilterable(
            level, description, constraints, create_replacement
        )
