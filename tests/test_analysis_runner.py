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

from datetime import datetime

import pandas as pd
from pandas.testing import assert_frame_equal
from tryingsnake import Success

from duckdq.core.properties import (
    Completeness,
    Maximum,
    Mean,
    Minimum,
    Size,
    StandardDeviation, ApproxDistinctness,
)
from duckdq.engines import PandasEngine
from duckdq.metadata.metadata_repository import InMemoryMetadataRepository
from duckdq.utils.analysis_runner import AnalyzerContext, do_analysis_run
from duckdq.core.metrics import DoubleMetric, Entity
from duckdq.utils.connection_handler import ConnectionHandler


class TestAnalysis:
    def test_return_result_for_configured_analyzers(self, df_full):
        analyzers = [
            Size(),
            Minimum("item"),
            Completeness("item"),
        ]

        engine = PandasEngine(df_full)
        repo = InMemoryMetadataRepository()

        ac = do_analysis_run(engine, repo, analyzers)

        sm = AnalyzerContext.success_metrics_as_dataframe(ac)

        expected = pd.DataFrame(
            [
                ("DATASET", "*", "Size", 4.0),
                ("COLUMN", "item", "Minimum", 1.0),
                ("COLUMN", "item", "Completeness", 1.0),
            ],
            columns=("entity", "instance", "name", "value"),
        )

        ConnectionHandler.close_connections()

        assert_frame_equal(sm, expected, check_like=True)



    def test_run_individual_analyzer_only_once(self, df_full):

        analyzers = [
            Minimum("item"),
            Minimum("item"),
            Minimum("item"),
        ]
        engine = PandasEngine(df_full)
        repo = InMemoryMetadataRepository()

        ac = do_analysis_run(engine, repo, analyzers)

        ConnectionHandler.close_connections()

        assert len(ac.all_metrics()) == 1
        metric = ac.metric(Minimum("item"))
        assert metric is not None
        assert metric.value.get() == 1

    def test_return_basic_statistics(self, df_with_numeric_values):
        df = df_with_numeric_values
        analyzers = [
            Mean("att1"),
            StandardDeviation("att1"),
            Minimum("att1"),
            Maximum("att1"),
            ApproxDistinctness("att1"),
            ApproxDistinctness("att2"),
        ]

        engine = PandasEngine(df_with_numeric_values)
        repo = InMemoryMetadataRepository()

        result_metrics = do_analysis_run(engine, repo, analyzers).all_metrics()

        ConnectionHandler.close_connections()

        assert len(result_metrics) == len(analyzers)

        assert (
            DoubleMetric(Entity.COLUMN, "Mean", "att1", Success(3.5)) in result_metrics
        )
        assert (
            DoubleMetric(Entity.COLUMN, "Minimum", "att1", Success(1.0))
            in result_metrics
        )
        assert (
                DoubleMetric(Entity.COLUMN, "ApproxDistinctness", "att1", Success(1.0))
                in result_metrics
        )
        assert (
                DoubleMetric(Entity.COLUMN, "ApproxDistinctness", "att2", Success(0.6666666716337205))
                in result_metrics
        )
        assert (
            DoubleMetric(Entity.COLUMN, "Maximum", "att1", Success(6.0))
            in result_metrics
        )
        assert (
            DoubleMetric(
                Entity.COLUMN, "StandardDeviation", "att1", Success(1.870829)
            )
            in result_metrics
        )

    def test_run_analyzers_with_different_where_conditions_separately(
        self, df_with_numeric_values
    ):
        df = df_with_numeric_values
        analyzers = [
            Maximum("att1"),
            Maximum("att1", where="att1 > att2"),
        ]

        engine = PandasEngine(df)
        repo = InMemoryMetadataRepository()

        ctx = do_analysis_run(engine, repo, analyzers)

        ConnectionHandler.close_connections()

        assert ctx.metric(analyzers[0]) == DoubleMetric(
            Entity.COLUMN, "Maximum", "att1", Success(6.0)
        )

        assert ctx.metric(analyzers[1]) == DoubleMetric(
            Entity.COLUMN, "Maximum", "att1", Success(3.0)
        )
