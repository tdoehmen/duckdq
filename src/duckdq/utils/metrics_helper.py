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

import traceback
from typing import Union, Dict

from tryingsnake import Success, Failure

from duckdq.core.metrics import DoubleMetric, SchemaMetric
from duckdq.core.properties import Entity, Property
from duckdq.utils.exceptions import EmptyStateException, NoMetricForValueException, MetricTypeNotSupportedException


def metric_from_value(
    value: Union[float, Dict[str,str]], name: str, instance: str, entity: Entity
) -> Union[DoubleMetric, SchemaMetric]:
    if isinstance(value, (float, int)):
        return DoubleMetric(entity, name, instance, Success(value))
    elif isinstance(value, dict):
        return SchemaMetric(entity, name, instance, Success(value))
    else:
        raise NoMetricForValueException(f"Can not create a Metric for value type {value.__class__.__name__}")


def metric_from_failure(
    ex: Exception, analyzer: Property
) -> Union[DoubleMetric, SchemaMetric]:
    name = analyzer.name
    instance = analyzer.instance
    entity = analyzer.entity

    # sometimes AssertionError does not contain any message let's add some context
    if isinstance(ex, AssertionError):
        summary = traceback.extract_tb(ex.__traceback__)
        # get the last ones
        ex.args += tuple(summary.format()[-2:])

    if analyzer.metric_type() == DoubleMetric:
        return DoubleMetric(entity, name, instance, Failure(ex))
    elif analyzer.metric_type() == SchemaMetric:
        return SchemaMetric(entity, name, instance, Failure(ex))
    else:
        raise MetricTypeNotSupportedException(f"Can not create an Exception Metric for {analyzer.metric_type().__name__}")


def metric_from_empty(
    analyzer: Property, name: str, instance: str, entity: Entity = Entity.COLUMN
):
    e = EmptyStateException(
        f"Empty state for analyzer {analyzer}, all input values were None."
    )
    return metric_from_failure(e, name, instance, entity)
