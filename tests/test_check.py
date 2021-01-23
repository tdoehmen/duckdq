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

# coding: utf-8
from datetime import datetime

import duckdq.utils.patterns as patterns
import pandas as pd
from duckdq.core.properties import Maximum, Mean, Minimum, Quantile, StandardDeviation, Sum
from duckdq.engines import PandasEngine
from duckdq.metadata.metadata_repository import InMemoryMetadataRepository
from duckdq.utils.analysis_runner import AnalyzerContext
from duckdq.utils.analysis_runner import do_analysis_run
from duckdq.checks import Check, CheckLevel, CheckStatus, is_one
from duckdq.constraints import ConstraintStatus
from duckdq.utils.connection_handler import ConnectionHandler


def run_checks(data, *checks) -> AnalyzerContext:
    analyzers = tuple([a for check in checks for a in check.required_analyzers()])
    engine = PandasEngine(data)
    repo = InMemoryMetadataRepository()
    result = do_analysis_run(engine, repo, analyzers)
    ConnectionHandler.close_connections()
    return result

def assert_evals_to(check: Check, context: AnalyzerContext, status: CheckStatus):
    assert check.evaluate(context).status == status


def is_success(check, context):
    return check.evaluate(context).status == CheckStatus.SUCCESS

class TestSchemaCheck:
    def test_column_exists(self, df_comp_incomp):
        df = df_comp_incomp

        check1 = (
            Check(CheckLevel.EXCEPTION, "group-1")
                .has_schema(lambda s: "item" in s)
        )

        check2 = (
            Check(CheckLevel.EXCEPTION, "group-2")
                .has_schema(lambda s: {"att1","att2","attr3"} <= s.keys())
        )

        check3 = (
            Check(CheckLevel.EXCEPTION, "group-3")
                .has_schema(lambda s: {"att1","att2"} <= s.keys())
        )

        check4 = (
            Check(CheckLevel.EXCEPTION, "group-4")
                .has_schema(lambda s: {"item": "BIGINT", "att1": "VARCHAR","att2":"VARCHAR"} == s)
        )

        context = run_checks(df, check1, check2, check3, check4)

        assert_evals_to(check1, context, CheckStatus.SUCCESS)
        assert_evals_to(check2, context, CheckStatus.ERROR)
        assert_evals_to(check3, context, CheckStatus.SUCCESS)
        assert_evals_to(check4, context, CheckStatus.SUCCESS)


class TestCompletenessCheck:
    def test_return_corect_status(self, df_comp_incomp):
        df = df_comp_incomp

        check1 = (
            Check(CheckLevel.EXCEPTION, "group-1")
            .is_complete("att1")
            .has_completeness("att1", lambda v: v == 1.0)  # 1.0
        )

        check2 = (
            Check(CheckLevel.EXCEPTION, "group-2-E")
            .is_complete("att2")
            .has_completeness("att1", lambda v: v > 0.8)  # 0.66
        )

        check3 = (
            Check(CheckLevel.WARNING, "group-2-W")
            .is_complete("att2")
            .has_completeness("att1", lambda v: v > 0.8)  # 0.66
        )

        context = run_checks(df, check1, check2, check3)

        assert_evals_to(check1, context, CheckStatus.SUCCESS)
        assert_evals_to(check2, context, CheckStatus.ERROR)
        assert_evals_to(check3, context, CheckStatus.WARNING)

    def test_where(self, df_comp_incomp):
        df = df_comp_incomp

        check1 = (
            Check(CheckLevel.EXCEPTION, "group-1")
                .is_complete("att2").where("item<3")
                .has_completeness("att2", lambda v: v == 0.5).where("item>=3")
        )

        check2 = (
            Check(CheckLevel.EXCEPTION, "group-2")
                .is_complete("att2").where("item<3")
                .has_completeness("att2", lambda v: v == 1.0).where("item<3")
        )

        context = run_checks(df, check1, check2)

        assert_evals_to(check1, context, CheckStatus.SUCCESS)
        assert_evals_to(check2, context, CheckStatus.SUCCESS)


class TestSizeCheck:
    def test_return_correct_status(self, df_comp_incomp):
        df = df_comp_incomp

        check1 = (
            Check(CheckLevel.EXCEPTION, "group-1")
                .has_size(lambda s: s == 6)
        )

        check2 = (
            Check(CheckLevel.EXCEPTION, "group-2")
                .has_size(lambda s: s > 6)
        )

        context = run_checks(df, check1, check2)

        assert_evals_to(check1, context, CheckStatus.SUCCESS)
        assert_evals_to(check2, context, CheckStatus.ERROR)

    def test_where(self, df_comp_incomp):
        df = df_comp_incomp

        check1 = (
            Check(CheckLevel.EXCEPTION, "group-1")
                .has_size(lambda s: s == 3).where("item>3")
        )

        context = run_checks(df, check1)

        assert_evals_to(check1, context, CheckStatus.SUCCESS)

class TestUniquenessCheckSimple:
    def test_return_correct_status(self, df_comp_incomp):
        df = df_comp_incomp

        check1 = (
            Check(CheckLevel.EXCEPTION, "group-1")
                .is_unique("item")
        )

        check2 = (
            Check(CheckLevel.EXCEPTION, "group-2")
                .is_unique("att1")
        )

        context = run_checks(df, check1)

        assert_evals_to(check1, context, CheckStatus.SUCCESS)
        assert_evals_to(check2, context, CheckStatus.ERROR)

    def test_where(self, df_comp_incomp):
        df = df_comp_incomp

        check1 = (
            Check(CheckLevel.EXCEPTION, "group-1")
                .is_unique("att1").where("item<3")
        )

        context = run_checks(df, check1)

        assert_evals_to(check1, context, CheckStatus.SUCCESS)

class TestDistinctnessCheckSimple:
    def test_return_correct_status(self, df_comp_incomp):
        df = df_comp_incomp

        check1 = (
            Check(CheckLevel.EXCEPTION, "group-1")
                .is_distinct("item")
        )

        check2 = (
            Check(CheckLevel.EXCEPTION, "group-2")
                .is_distinct("att1")
        )

        context = run_checks(df, check1)

        assert_evals_to(check1, context, CheckStatus.SUCCESS)
        assert_evals_to(check2, context, CheckStatus.ERROR)

    def test_where(self, df_comp_incomp):
        df = df_comp_incomp

        check1 = (
            Check(CheckLevel.EXCEPTION, "group-1")
                .is_distinct("att1").where("item<3")
        )

        context = run_checks(df, check1)

        assert_evals_to(check1, context, CheckStatus.SUCCESS)

class TestChecksOnBasicStats:
    def test_yield_correct_results(self, df_with_numeric_values):

        df = df_with_numeric_values

        base_check = Check(CheckLevel.EXCEPTION, description="a description")
        analyzers = [
            Minimum("att1"),
            Maximum("att1"),
            Mean("att1"),
            StandardDeviation("att1"),
            Sum("att1"),
            Quantile("att1", 0.5),
        ]

        engine = PandasEngine(df)
        repo = InMemoryMetadataRepository()
        context_numeric = do_analysis_run(engine, repo, analyzers)

        assert is_success(
            base_check.has_min("att1", lambda v: v == 1.0), context_numeric
        )
        assert is_success(
            base_check.has_max("att1", lambda v: v == 6.0), context_numeric
        )
        assert is_success(
            base_check.has_mean("att1", lambda v: v == 3.5), context_numeric
        )
        assert is_success(
            base_check.has_standard_deviation("att1", lambda v: v == 1.870829),
            context_numeric,
        )
        assert is_success(
            base_check.has_sum("att1", lambda v: v == 21.0), context_numeric
        )
        assert is_success(
            base_check.has_approx_quantile("att1", 0.5, lambda v: v == 4.0), context_numeric
        )

    def test_multiple_quantiles_are_computed(self, df_with_numeric_values):
        df = df_with_numeric_values
        analyzers = [
            Quantile("att1", 0.1),
            Quantile("att1", 0.5),
            Quantile("att1", 0.9),
        ]
        engine = PandasEngine(df)
        repo = InMemoryMetadataRepository()
        context_numeric = do_analysis_run(engine, repo, analyzers)
        assert len(context_numeric.metric_map) == 3

        print(context_numeric)
        base_check = Check(CheckLevel.EXCEPTION, description="a description")

        assert is_success(
            base_check.has_approx_quantile("att1", 0.5, lambda v: v == 4.0), context_numeric
        )
        assert is_success(
            base_check.has_approx_quantile("att1", 0.9, lambda v: v == 6.0), context_numeric
        )
        assert is_success(
            base_check.has_approx_quantile("att1", 0.1, lambda v: v == 1.0), context_numeric
        )

    def test_correctly_evaluate_mean_constraints(self, df_with_numeric_values):

        df = df_with_numeric_values
        mean_check = Check(CheckLevel.EXCEPTION, "a").has_mean("att1", lambda v: v == 3.5)

        mean_check_with_filter = (
            Check(CheckLevel.EXCEPTION, "a")
            .has_mean("att1", lambda v: v == 5.0)
            .where("att2 > 0")
        )

        ctx = run_checks(df, mean_check, mean_check_with_filter)

        assert is_success(mean_check, ctx)
        assert is_success(mean_check_with_filter, ctx)

    def test_correctly_evaluate_size_constraint(self, df_with_numeric_values):
        df = df_with_numeric_values
        nrows = len(df)

        check1 = Check(CheckLevel.EXCEPTION, "group-1-S-1").has_size(lambda r: r == nrows)
        check2 = Check(CheckLevel.WARNING, "group-1-S-2").has_size(lambda r: r == nrows)
        check3 = Check(CheckLevel.EXCEPTION, "group-1-E").has_size(lambda r: r != nrows)
        check4 = Check(CheckLevel.WARNING, "group-1-W").has_size(lambda r: r != nrows)
        check5 = Check(CheckLevel.WARNING, "group-1-W-range").has_size(
            lambda r: r > 0 and r < nrows + 1
        )

        context = run_checks(df, check1, check2, check3, check4, check5)

        assert_evals_to(check1, context, CheckStatus.SUCCESS)
        assert_evals_to(check2, context, CheckStatus.SUCCESS)
        assert_evals_to(check3, context, CheckStatus.ERROR)
        assert_evals_to(check4, context, CheckStatus.WARNING)
        assert_evals_to(check5, context, CheckStatus.SUCCESS)


class TestSatifiesCheck:
    def test_return_correct_check_status_for_col_constraints(
        self, df_with_numeric_values
    ):

        df = df_with_numeric_values

        check1 = Check(CheckLevel.EXCEPTION, "group-1").satisfies("att1 > 0", "rule1")

        check2 = Check(CheckLevel.EXCEPTION, "group-2-to-fail").satisfies(
            "att1 > 3", "rule2"
        )

        check3 = Check(CheckLevel.EXCEPTION, "group-2-to-succeed").satisfies(
            "att1 > 3", "rule3", lambda v: v == 0.5
        )

        context = run_checks(df, check1, check2, check3)

        assert_evals_to(check1, context, CheckStatus.SUCCESS)
        assert_evals_to(check2, context, CheckStatus.ERROR)
        assert_evals_to(check3, context, CheckStatus.SUCCESS)

    def test_return_correct_check_status_for_col_constraints_with_cond(
        self, df_with_numeric_values
    ):

        df = df_with_numeric_values

        check_succeed = (
            Check(CheckLevel.EXCEPTION, "group-1")
            .satisfies("att1 < att2", "rule1")
            .where("att1 > 3")
        )

        check_fail = (
            Check(CheckLevel.EXCEPTION, "group-1")
            .satisfies("att2 > 0", "rule2")
            .where("att1 > 0")
        )

        check_partially_satisfied = (
            Check(CheckLevel.EXCEPTION, "group-1")
            .satisfies("att2 > 0", "rule3", lambda v: v == 0.5)
            .where("att1 > 0")
        )

        context = run_checks(df, check_succeed, check_fail, check_partially_satisfied)

        assert_evals_to(check_succeed, context, CheckStatus.SUCCESS)
        assert_evals_to(check_fail, context, CheckStatus.ERROR)
        assert_evals_to(check_partially_satisfied, context, CheckStatus.SUCCESS)

    def test_correctly_evaluate_non_negative_and_positive_constraints(
        self, df_with_numeric_values
    ):
        df = df_with_numeric_values

        nn_check = Check(CheckLevel.EXCEPTION, "a").is_non_negative("att1")
        pos_check = Check(CheckLevel.EXCEPTION, "a").is_positive("att1")

        context = run_checks(df, nn_check, pos_check)

        assert_evals_to(nn_check, context, CheckStatus.SUCCESS)
        assert_evals_to(pos_check, context, CheckStatus.SUCCESS)

    def test_correctly_evaluate_is_contained_constraints(self, df_with_distinct_values):
        df = df_with_distinct_values

        range_check = Check(CheckLevel.EXCEPTION, "a").is_contained_in(
            "att1", ("a", "b", "c")
        )

        incorrect_range_check = Check(CheckLevel.EXCEPTION, "a").is_contained_in(
            "att1", ("a", "b")
        )
        incorrect_range_check_with_assertion = Check(
            CheckLevel.EXCEPTION, "a"
        ).is_contained_in("att1", ("a",), lambda v: v == 0.5)

        range_results = run_checks(
            df,
            range_check,
            incorrect_range_check,
            incorrect_range_check_with_assertion,
        )

        assert_evals_to(range_check, range_results, CheckStatus.SUCCESS)
        assert_evals_to(incorrect_range_check, range_results, CheckStatus.ERROR)
        assert_evals_to(
            incorrect_range_check_with_assertion, range_results, CheckStatus.SUCCESS
        )

    def test_correctly_evaluate_is_contained_in_range_constraints(
        self, df_with_numeric_values,
    ):

        df = df_with_numeric_values

        numeric_range_check1 = Check(CheckLevel.EXCEPTION, "nr1").is_contained_in_range(
            "att2", 0, 7
        )

        numeric_range_check2 = Check(CheckLevel.EXCEPTION, "nr2").is_contained_in_range(
            "att2", 1, 7
        )

        numeric_range_check3 = Check(CheckLevel.EXCEPTION, "nr3").is_contained_in_range(
            "att2", 0, 6
        )

        numeric_range_check4 = Check(CheckLevel.EXCEPTION, "nr4").is_contained_in_range(
            "att2", 0, 7, include_lower_bound=False, include_upper_bound=False
        )

        numeric_range_check5 = Check(CheckLevel.EXCEPTION, "nr5").is_contained_in_range(
            "att2", -1, 8, include_lower_bound=False, include_upper_bound=False
        )

        numeric_range_check6 = Check(CheckLevel.EXCEPTION, "nr6").is_contained_in_range(
            "att2", 0, 7, include_lower_bound=True, include_upper_bound=False
        )

        numeric_range_check7 = Check(CheckLevel.EXCEPTION, "nr7").is_contained_in_range(
            "att2", 0, 8, include_lower_bound=True, include_upper_bound=False
        )

        numeric_range_check8 = Check(CheckLevel.EXCEPTION, "nr8").is_contained_in_range(
            "att2", 0, 7, include_lower_bound=False, include_upper_bound=True
        )

        numeric_range_check9 = Check(CheckLevel.EXCEPTION, "nr9").is_contained_in_range(
            "att2", -1, 7, include_lower_bound=False, include_upper_bound=True
        )

        numeric_range_results = run_checks(
            df,
            numeric_range_check1,
            numeric_range_check2,
            numeric_range_check3,
            numeric_range_check4,
            numeric_range_check5,
            numeric_range_check6,
            numeric_range_check7,
            numeric_range_check8,
            numeric_range_check9,
        )

        assert_evals_to(
            numeric_range_check1, numeric_range_results, CheckStatus.SUCCESS
        )
        assert_evals_to(numeric_range_check2, numeric_range_results, CheckStatus.ERROR)
        assert_evals_to(numeric_range_check3, numeric_range_results, CheckStatus.ERROR)
        assert_evals_to(numeric_range_check4, numeric_range_results, CheckStatus.ERROR)

        assert_evals_to(
            numeric_range_check5, numeric_range_results, CheckStatus.SUCCESS
        )

        assert_evals_to(numeric_range_check6, numeric_range_results, CheckStatus.ERROR)
        assert_evals_to(
            numeric_range_check7, numeric_range_results, CheckStatus.SUCCESS
        )

        assert_evals_to(numeric_range_check8, numeric_range_results, CheckStatus.ERROR)
        assert_evals_to(
            numeric_range_check9, numeric_range_results, CheckStatus.SUCCESS
        )


class TestUniquenessCheck:
    def test_return_the_correct_check_status(self, df_with_unique_columns):
        df = df_with_unique_columns

        check = (
            Check(CheckLevel.EXCEPTION, "group-1-u")
            .has_uniqueness("nonUnique", lambda fraction: fraction == 0.5)
            .has_uniqueness("nonUnique", lambda fraction: fraction < 0.6)
            .has_uniqueness(
                ("halfUniqueCombinedWithNonUnique", "nonUnique"),
                lambda fraction: fraction == 0.5,
            )
            .has_uniqueness(("onlyUniqueWithOtherNonUnique", "nonUnique"), is_one,)
            .has_uniqueness("uniqueCol", is_one)
            .has_uniqueness("uniqueWithNulls", is_one)
            .has_uniqueness(("nonUnique", "halfUniqueCombinedWithNonUnique"), is_one)
            .where("nonUnique > 0")
            .has_uniqueness(
                ("nonUnique", "halfUniqueCombinedWithNonUnique"), is_one, "hint"
            )
            .where("nonUnique > 0")
            .has_uniqueness("halfUniqueCombinedWithNonUnique", is_one)
            .where("nonUnique > 0")
            .has_uniqueness("halfUniqueCombinedWithNonUnique", is_one, "hint")
            .where("nonUnique > 0")
        )

        context = run_checks(df, check)

        result = check.evaluate(context)

        assert result.status == CheckStatus.SUCCESS

        statuses = [cr.status for cr in result.constraint_results]

        #  Half of nonUnique column are duplicates
        assert statuses[0] == ConstraintStatus.SUCCESS
        assert statuses[1] == ConstraintStatus.SUCCESS

        # Half of the 2 columns are duplicates as well.
        assert statuses[2] == ConstraintStatus.SUCCESS

        # Both next 2 cases are actually unique so should meet threshold
        assert statuses[3] == ConstraintStatus.SUCCESS
        assert statuses[4] == ConstraintStatus.SUCCESS

        # Nulls are duplicated so this will not be unique
        assert statuses[5] == ConstraintStatus.SUCCESS

        # Multi-column uniqueness, duplicates filtered out
        assert statuses[6] == ConstraintStatus.SUCCESS

        # Multi-column uniqueness with hint, duplicates filtered out
        assert statuses[7] == ConstraintStatus.SUCCESS

        # Single-column uniqueness, duplicates filtered out
        assert statuses[8] == ConstraintStatus.SUCCESS

        # Single-column uniqueness with hint, duplicates filtered out
        assert statuses[9] == ConstraintStatus.SUCCESS


class TestPatternMatchCheck:
    def test_has_pattern_work_with_normal_patterns(self,):
        col = "someCol"
        df = pd.DataFrame({col: ["someone@somewhere.org", "someone@else.com"]})

        check = Check(CheckLevel.EXCEPTION, "some description").has_pattern(
            col, patterns.EMAIL
        )
        context = run_checks(df, check)
        assert_evals_to(check, context, CheckStatus.SUCCESS)

    def test_fail_on_mixed_data_for_email(self,):
        col = "someCol"
        df = pd.DataFrame({col: ["someone@somewhere.org", "someone@else"]})

        check = Check(CheckLevel.EXCEPTION, "some description").has_pattern(
            col, patterns.EMAIL
        )
        context = run_checks(df, check)
        assert_evals_to(check, context, CheckStatus.ERROR)

    def test_on_regular_expression_patterns_for_urls(self,):
        col = "someCol"
        df = pd.DataFrame(
            {
                col: [
                    "https://www.example.com/foo/?bar=baz&inga=42&quux",
                    "https://foo.bar/baz",
                ]
            }
        )
        check = Check(CheckLevel.EXCEPTION, "some description").has_pattern(
            col, patterns.URL
        )
        context = run_checks(df, check)
        assert_evals_to(check, context, CheckStatus.SUCCESS)

    def test_work_on_regular_expression_with_filtering(self,):

        df = pd.DataFrame(
            {
                "value": ["someone@somewhere.org", "someone@else"],
                "type": ["valid", "invalid"],
            }
        )

        check = Check(CheckLevel.EXCEPTION, "some description").has_pattern(
            "value", patterns.EMAIL, lambda v: v == 0.5
        )

        check_with_filter = (
            Check(CheckLevel.EXCEPTION, "some description")
            .has_pattern("value", patterns.EMAIL, lambda v: v == 1.0)
            .where("type == 'valid'")
        )

        context = run_checks(df, check, check_with_filter)

        assert_evals_to(check, context, CheckStatus.SUCCESS)
        assert_evals_to(check_with_filter, context, CheckStatus.SUCCESS)

    def test_fails_on_mixed_data_for_url_pattern(self,):
        col = "someCol"
        df = pd.DataFrame(
            {
                col: [
                    "https://www.example.com/foo/?bar=baz&inga=42&quux",
                    "http:// shouldfail.com",
                ],
            }
        )
        check = Check(CheckLevel.EXCEPTION, "some description").has_pattern(
            col, patterns.URL
        )
        context = run_checks(df, check)
        assert_evals_to(check, context, CheckStatus.ERROR)

    def test_contains_credit_card_number(self,):
        df = pd.DataFrame(
            {
                "value": ["4111 1111 1111 1111", "9999888877776666"],
                "type": ["valid", "invalid"],
            }
        )
        check = Check(CheckLevel.EXCEPTION, "some description").contains_credit_card_number(
            "value", lambda v: v == 0.5
        )

        check_with_filter = (
            Check(CheckLevel.EXCEPTION, "some description")
            .contains_credit_card_number("value", lambda v: v == 1.0)
            .where("type == 'valid'")
        )

        context = run_checks(df, check, check_with_filter)
        assert_evals_to(check, context, CheckStatus.SUCCESS)
        assert_evals_to(check_with_filter, context, CheckStatus.SUCCESS)

    def test_contains_email(self,):
        df = pd.DataFrame(
            {
                "value": ["someone@somewhere.org", "someone@else"],
                "type": ["valid", "invalid"],
            }
        )
        check = Check(CheckLevel.EXCEPTION, "some description").contains_email(
            "value", lambda v: v == 0.5
        )

        check_with_filter = (
            Check(CheckLevel.EXCEPTION, "some description")
            .contains_email("value", lambda v: v == 1.0)
            .where("type == 'valid'")
        )

        context = run_checks(df, check, check_with_filter)
        assert_evals_to(check, context, CheckStatus.SUCCESS)
        assert_evals_to(check_with_filter, context, CheckStatus.SUCCESS)

    def test_contains_url(self,):
        df = pd.DataFrame(
            {
                "value": [
                    "https://www.example.com/foo/?bar=baz&inga=42&quux",
                    "http:// shouldfail.com",
                ],
                "type": ["valid", "invalid"],
            }
        )
        check = Check(CheckLevel.EXCEPTION, "some description").contains_url(
            "value", lambda v: v == 0.5
        )

        check_with_filter = (
            Check(CheckLevel.EXCEPTION, "some description")
            .contains_url("value", lambda v: v == 1.0)
            .where("type == 'valid'")
        )

        context = run_checks(df, check, check_with_filter)
        assert_evals_to(check, context, CheckStatus.SUCCESS)
        assert_evals_to(check_with_filter, context, CheckStatus.SUCCESS)
