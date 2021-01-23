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

import logging
import uuid
from dataclasses import dataclass
from datetime import datetime
from typing import List, Mapping, Optional, Sequence, Tuple

from duckdq.utils.analysis_runner import do_analysis_run, AnalyzerContext
from duckdq.checks import Check, CheckResult, CheckStatus
from duckdq.core.properties import Property
from duckdq.utils.dataframe import DataFrame
from duckdq.engines import PandasEngine, SQLEngineFactory
from duckdq.engines.engine import Engine
from duckdq.core.metrics import Metric
from duckdq.metadata.metadata_repository import InMemoryMetadataRepository, SQLMetadataRepositoryFactory, \
    MetadataRepository
from duckdq.utils.connection_handler import ConnectionHandler

logger = logging.getLogger(__name__)

@dataclass
class VerificationResult:
    status: CheckStatus
    check_results: Mapping[Check, CheckResult]
    metrics: Mapping[Property, Metric]

    def __str__(self):
        check_results_s = ""
        for check, check_result in self.check_results.items():
            check_results_s += f"{check.description}: {check_result.status.name}\n"
            for constraint_result in check_result.constraint_results:
                constraint_s = constraint_result.constraint.__str__()
                constraint_result_s = constraint_result.status.name
                metric_value = constraint_result.metric.value
                metric_s = metric_value.get() if metric_value.isSuccess else metric_value
                check_results_s += f"    {constraint_s}: {constraint_result_s} ({metric_s})\n"
        return check_results_s



# Helper for the fluent Api
class VerificationRunBuilder:
    def __init__(self, engine: Engine, dataset_id: str = None, partition_id: str = None):
        self.engine: Engine = engine
        self.dataset_id = dataset_id
        self.partition_id = partition_id
        self.repo = InMemoryMetadataRepository()
        self.repo.set_dataset(dataset_id, partition_id)
        self._checks: List[Check] = []
        self._required_analyzers: Optional[Tuple[Property , ...]] = None

    def using_metadata_repository(self, url_or_con):
        self.repo = SQLMetadataRepositoryFactory.create_sql_metadata_repository(url_or_con)
        self.repo.set_dataset(self.dataset_id, self.partition_id)
        return self

    def run(self) -> VerificationResult:

        return VerificationSuite().do_verification_run(
            self.engine, self.repo, self._checks, self._required_analyzers
        )

    def add_check(self, check: Check) -> "VerificationRunBuilder":
        """
        Add a single check to the run.

        Parameters
        ----------

        check:
             A check object to be executed during the run
        """
        self._checks.append(check)
        return self

    def add_checks(self, checks: Sequence[Check]) -> "VerificationRunBuilder":
        """
        Add multiple checks to the run.

        Parameters
        ----------

        checks:
             A sequence of check objects to be executed during the run
        """
        self._checks.extend(checks)
        return self

    def load_checks(self):
        pass


class VerificationSuite:
    def __init__(self):
        self._checks: List[Check] = []
        self._required_analyzers: Optional[Tuple[Property, ...]] = None

    def add_check(self, check: Check) -> "VerificationSuite":
        """
        Add a single check to the run.

        Parameters
        ----------

        check:
             A check object to be executed during the run
        """

        self._checks.append(check)
        return self

    def add_checks(self, checks: Sequence[Check]) -> "VerificationSuite":
        """
        Add multiple checks to the run.

        Parameters
        ----------

        checks:
             A sequence of check objects to be executed during the run
        """

        self._checks.extend(checks)
        return self

    def run(self, data: DataFrame, dataset_id: str = None, partition_id: str = None) -> VerificationResult: #TODO: maybe drop this function
        """
        Runs all check groups and returns the verification result.
        Verification result includes all the metrics computed during the run.

        Parameters
        ----------

        data:
             tabular data on which the checks should be verified
        """
        engine = PandasEngine(data)
        repo = InMemoryMetadataRepository()
        repo.set_dataset(dataset_id,partition_id)
        return self.do_verification_run(
            engine, repo, self._checks, self._required_analyzers
        )

    def on_data(self, data: DataFrame, dataset_id: str = None, partition_id: str = None):
        engine = PandasEngine(data)
        return VerificationRunBuilder(engine, dataset_id, partition_id)

    def on_table(self, url_or_con, table: str, dataset_id: str = None, partition_id: str = None):
        engine = SQLEngineFactory.create_sql_engine(url_or_con, table)
        return VerificationRunBuilder(engine, dataset_id, partition_id)

    def do_verification_run(
        self,
        engine: Engine,
        repo: MetadataRepository,
        checks: Sequence[Check],
        required_analyzers: Optional[Tuple[Property, ...]] = None,
    ) -> VerificationResult:
        """

        Runs all check groups and returns the verification result.
        Verification result includes all the metrics computed during the run.

        Parameters
        ----------

        data:
            tabular data on which the checks should be verified
        checks:
           A sequence of check objects to be executed
        required_analyzers:
           Can be used to enforce the calculation of some some metrics
           regardless of if there are constraints on them (optional)
        aggregate_with: not implemented
            loader from which we retrieve initial states to aggregate (optional)
        save_states_with: not implemented
            persist resulting states for the configured analyzers (optional)
        metrics_repository_options:
            Options related to the MetricsRepository

        Returns
        --------
        returns Result for every check including the overall status, detailed status
        for each constraints and all metrics produced

        """
        required_analyzers = required_analyzers or ()
        analyzers = required_analyzers + tuple(
            [a for check in checks for a in check.required_analyzers()]
        )

        # This rhis returns AnalysisContext
        analysis_result = do_analysis_run(engine, repo, analyzers)

        verification_result = self.evaluate(checks, analysis_result)

        # TODO: Save ave or append Results on the metric reposiotory
        # TODO: Save JsonOutputToFilesystemIfNecessary
        # pull up store_metrics from do_analysis_run() and include precondition metrics
        repo.store_checks(checks)

        ConnectionHandler.close_connections()
        return verification_result

    def evaluate(
        self, checks: Sequence[Check], analysis_context: AnalyzerContext,
    ) -> VerificationResult:

        check_results = {c: c.evaluate(analysis_context) for c in checks}
        if not check_results:
            verification_status = CheckStatus.SUCCESS
        else:
            verification_status = max(cr.status for cr in check_results.values())

        return VerificationResult(
            verification_status, check_results, analysis_context.metric_map
        )
