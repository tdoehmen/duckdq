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

import math
from abc import ABCMeta, abstractmethod, ABC
from dataclasses import dataclass

from pandas import DataFrame
from typing import List, TypeVar, Generic, Optional, Dict


@dataclass()
class State(ABC):
    id: str

class SerializableState(State):
    pass

class TabularState(State):
    has_df: bool = False

    def set_df(self, df: DataFrame):
        self.df = df
        self.has_df = True

    @abstractmethod
    def get_table_name(self) -> str:
        pass

@dataclass()
class SchemaState(SerializableState):
    schema: Dict[str,str]

@dataclass()
class MaxState(SerializableState):
    max_value: float

@dataclass()
class MeanState(SerializableState):
    total: float
    count: int

@dataclass()
class MinState(SerializableState):
    min_value: float

@dataclass()
class NumMatches(SerializableState):
    num_matches: int

@dataclass()
class NumMatchesAndCount(SerializableState):
    num_matches: int
    count: int

@dataclass()
class QuantileState(SerializableState):
    serializedKll: str
    quantile: float
    sketch_type: str

@dataclass()
class ApproxDistinctState(SerializableState):
    serializedHll: str
    estimate: float
    num_rows: int

@dataclass()
class StandardDeviationState(SerializableState):
    n: float
    avg: float
    m2: float
    stddev: float

@dataclass()
class SumState(SerializableState):
    sum_value: float

@dataclass()
class FrequenciesAndNumRows(TabularState):
    frequencies_table: str
    grouping_columns: List[str]
    num_rows: int

    def get_table_name(self) -> str:
        return self.frequencies_table
