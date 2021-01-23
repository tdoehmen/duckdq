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
from dataclasses import dataclass
from enum import Enum
from typing import TypeVar, Generic, Sequence, Mapping, Union, Optional, List, Dict
from tryingsnake import Try_, Failure, Success

T = TypeVar("T")

class Entity(Enum):
    DATASET = 1
    COLUMN = 2
    TWOCOLUMN = 3
    MULTICOLUMN = 4

@dataclass(frozen=True)
class Metric(Generic[T]):
    entity: Entity
    name: str
    instance: str
    value: Try_

    def flatten(self) -> Sequence["Metric[T]"]:
        pass

    # This would replace simplifiedMetricOutput
    def asdict(self) -> Mapping[str, Union[str, Optional[float], Optional[Dict[str,str]]]]: #TODO: why optional?
        return {
            "entity": str(self.entity).split(".")[-1],
            "instance": self.instance,
            "name": self.name,
            "value": self.value.getOrElse(None),
        }


class DoubleMetric(Metric[float]):
    def flatten(self) -> Sequence[Metric[float]]:
        return (self,)


class SchemaMetric(Metric[Dict[str,str]]):
    def flatten(self) -> Sequence[Metric[Dict[str,str]]]:
        return (self,)

