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

import duckdb
import pandas as pd

from duckdq.core.metrics import Metric
from duckdq.core.properties import Completeness, Property, Uniqueness, Distinctness, Schema, UniqueValueRatio
from duckdq.engines.sql.sql_engine import DuckDBEngine
from duckdq.metadata.metadata_repository import InMemoryMetadataRepository
from duckdq.utils.analysis_runner import do_analysis_run

def compute_metric(df: pd.DataFrame, property: Property) -> Metric:
    con = duckdb.connect(":memory:")
    con.from_df(df).create("test")
    engine = DuckDBEngine(con,"test")
    repo = InMemoryMetadataRepository()
    metrics = engine.compute_metrics(set([property]),repo)
    con.close()
    return metrics[property]

class TestSQLOperators():

    def test_completeness_operators(self):
        df = pd.DataFrame(
            [
                (1, "Thingy A", "awesome thing.", "high", 0),
                (2, "Thingy B", "available at http://thingb.com", None, 0),
                (3, None, None, "low", 5),
                (4, "Thingy D", "checkout https://thingd.ca", "low", 10),
                (5, "Thingy E", None, "high", 12),
            ],
            columns=["id", "productName", "description", "priority", "numViews"],
        )
        property = Completeness("productName")
        metric = compute_metric(df, property)

        assert metric.value.get() == 0.8

    def test_distinctness_operators(self):
        df = pd.DataFrame(
            [
                (1, "Thingy A", "awesome thing.", "high", 0),
                (2, "Thingy B", "available at http://thingb.com", None, 0),
                (3, "Thingy B", None, "low", 5),
                (4, "Thingy C", "checkout https://thingd.ca", "low", 10),
                (5, "Thingy C", None, "high", 12),
            ],
            columns=["id", "productName", "description", "priority", "numViews"],
        )
        property = Distinctness(["productName"])
        metric = compute_metric(df, property)

        assert metric.value.get() == 3/5

        df = pd.DataFrame(
            [
                (1, "Thingy A", "awesome thing.", "high", 0),
                (2, "Thingy B", "available at http://thingb.com", None, 0),
                (3, "Thingy B", None, "low", 5),
                (4, "Thingy C", "checkout https://thingd.ca", "low", 10),
                (5, "Thingy C", None, "high", 12),
            ],
            columns=["id", "productName", "description", "priority", "numViews"],
        )
        property = Distinctness(["id"])
        metric = compute_metric(df, property)

        assert metric.value.get() == 1


    def test_uniqueness_operators(self):
        df = pd.DataFrame(
            [
                (1, "Thingy A", "awesome thing.", "high", 0),
                (2, "Thingy B", "available at http://thingb.com", None, 0),
                (3, "Thingy B", None, "low", 5),
                (4, "Thingy C", "checkout https://thingd.ca", "low", 10),
                (5, "Thingy C", None, "high", 12),
            ],
            columns=["id", "productName", "description", "priority", "numViews"],
        )
        property = Uniqueness(["productName"])
        metric = compute_metric(df, property)

        assert metric.value.get() == 1/5

        df = pd.DataFrame(
            [
                (1, "Thingy A", "awesome thing.", "high", 0),
                (2, "Thingy B", "available at http://thingb.com", None, 0),
                (3, "Thingy B", None, "low", 5),
                (4, "Thingy C", "checkout https://thingd.ca", "low", 10),
                (5, "Thingy C", None, "high", 12),
            ],
            columns=["id", "productName", "description", "priority", "numViews"],
        )
        property = Distinctness(["id"])
        metric = compute_metric(df, property)

        assert metric.value.get() == 1

    def test_unique_value_ratio_operators(self):
        df = pd.DataFrame(
            [
                (1, "Thingy A", "awesome thing.", "high", 0),
                (2, "Thingy B", "available at http://thingb.com", None, 0),
                (3, "Thingy B", None, "low", 5),
                (4, "Thingy C", "checkout https://thingd.ca", "low", 10),
                (5, "Thingy C", None, "high", 12),
            ],
            columns=["id", "productName", "description", "priority", "numViews"],
        )
        property = UniqueValueRatio(["productName"])
        metric = compute_metric(df, property)

        assert metric.value.get() == 1/3

        df = pd.DataFrame(
            [
                (1, "Thingy A", "awesome thing.", "high", 0),
                (2, "Thingy B", "available at http://thingb.com", None, 0),
                (3, "Thingy B", None, "low", 5),
                (4, "Thingy C", "checkout https://thingd.ca", "low", 10),
                (5, "Thingy C", None, "high", 12),
            ],
            columns=["id", "productName", "description", "priority", "numViews"],
        )
        property = UniqueValueRatio(["id"])
        metric = compute_metric(df, property)

        assert metric.value.get() == 1


    def test_column_names_operators(self):
        df = pd.DataFrame(
            [
                (1, "Thingy A", "awesome thing.", "high", 0),
                (2, "Thingy B", "available at http://thingb.com", None, 0),
                (3, None, None, "low", 5),
                (4, "Thingy D", "checkout https://thingd.ca", "low", 10),
                (5, "Thingy E", None, "high", 12),
            ],
            columns=["id", "productName", "description", "priority", "numViews"],
        )
        property = Schema()
        metric = compute_metric(df, property)

        assert metric.value.get() == {"id":"BIGINT", "productName":"VARCHAR", "description":"VARCHAR", "priority":"VARCHAR", "numViews":"BIGINT"}
