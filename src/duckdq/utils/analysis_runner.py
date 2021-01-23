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

from collections import defaultdict
from dataclasses import dataclass, field
from itertools import accumulate
from typing import Dict, List, Mapping, Optional, Sequence, Set, cast

import pandas as pd

from duckdq.core.preconditions import find_first_failing
from duckdq.utils.metrics_helper import metric_from_failure
from duckdq.core.properties import Property
from duckdq.engines.engine import Engine
from duckdq.core.metrics import Metric
from duckdq.metadata.metadata_repository import MetadataRepository


@dataclass(frozen=True, eq=True)
class AnalyzerContext:
    metric_map: Mapping[Property, Metric] = field(default_factory=dict)

    def all_metrics(self) -> List[Metric]:
        return list(self.metric_map.values())

    def __add__(self, other: "AnalyzerContext"):
        return AnalyzerContext({**self.metric_map, **other.metric_map})

    def metric(self, analyzer: Property) -> Optional[Metric]:
        return self.metric_map.get(analyzer, None)

    @classmethod
    def success_metrics_as_dataframe(
        cls,
        analyzer_context: "AnalyzerContext",
        for_analyzers: Sequence[Property] = None,
    ) -> pd.DataFrame:

        if not for_analyzers:
            for_analyzers = []

        mp = analyzer_context.metric_map

        # originally implemented in getSimplifiedOutputForSelectedAnalyzers

        # Get the analyzers are required that were sucessful
        mp = {
            k: mp[k]
            for k in mp
            if (
                (not len(for_analyzers) or k in for_analyzers) and mp[k].value.isSuccess
            )
        }

        # Get metrics as Double and replace simple name with description
        renamed: List[Metric] = []
        for a in mp:
            # TODO: rename metric
            renamed.extend(map(lambda x: x, mp[a].flatten()))

        df = pd.DataFrame(metric.asdict() for metric in renamed)
        df = df.sort_values(by="entity", ascending=False, ignore_index=True)
        return df


def do_analysis_run(
    engine: Engine,
    repo: MetadataRepository,
    analyzers: Sequence[Property]
) -> AnalyzerContext:
    """

    Compute the metrics from the analyzers configured in the analysis

    Parameters
    ----------

    data:
         data on which to operate
    analyzers:
         the analyzers to run
    aggregate_With: (not implemented)
         load existing states for the configured analyzers
         and aggregate them (optional)
    save_States_With: (not implemented)
        persist resulting states for the configured analyzers (optional)
    metric_repository_options: (not implemented)
        options related to the MetricsRepository
    file_output_options: (not implemented probably will be removed)
        options related to File Ouput.

    Returns
    -------
    An AnalyzerContext holding the requested metrics per analyzer
    """

    if not analyzers:
        return AnalyzerContext()

    # TODO:
    # If we already calculated some metric and they are in the metric repo
    # we will take it from the metric repo instead.
    # relevant in the case of multiple checks  on the same datasource_
    # also do some additional checks here

    # for now they are the same
    analyzers_to_run: Sequence[Property] = analyzers
    passed_analyzers: Sequence[Property] = list(
        filter(
            lambda an: find_first_failing(engine, an.preconditions()) is None,
            analyzers_to_run,
        )
    )
    # Create the failure metrics from the precondition violations

    failed_analyzers: Set[Property] = set(analyzers_to_run) - set(passed_analyzers)
    precondition_failures = compute_precondition_failure_metrics(failed_analyzers, engine)

    # Originally the idea is be able to run all the analysis on a single scan
    # assuming that internally pandas would do something like that
    # however apparently there is no big gain from running all aggregations at once
    # so for now we run the aggregation sequentially.

    # TODO: Deal with gruping analyzers (if necessary)
    metrics = run_analyzers_sequentially(engine, repo, analyzers)

    repo.store_metrics(metrics.metric_map)

    return metrics + precondition_failures


def compute_precondition_failure_metrics(
    failed_analyzers: Set[Property], engine: Engine
) -> AnalyzerContext:
    def first_exception_to_failure_metric(analyzer: Property):
        first_exception = find_first_failing(engine, analyzer.preconditions())
        if not first_exception:
            raise AssertionError("At least one exception should be found in a failing")

        return metric_from_failure(first_exception, analyzer)

    failures = {a: first_exception_to_failure_metric(a) for a in failed_analyzers}
    return AnalyzerContext(failures)


def run_analyzers_sequentially(
    engine: Engine, repo: MetadataRepository, analyzers: Sequence[Property]
) -> AnalyzerContext:
    """
    Apparently from the initial tests I made there is not a lot of gain from
    running all the aggregations at once.
    """

    if not len(analyzers):
        return AnalyzerContext()

    metrics_by_analyzer: Dict[Property, Metric] = engine.compute_metrics(analyzers, repo)

    analyzer_context = AnalyzerContext(metrics_by_analyzer)

    return analyzer_context
