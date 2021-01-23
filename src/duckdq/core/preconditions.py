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

"""Preconditions are tested before the analysis is run"""
from abc import ABC
from dataclasses import dataclass
from typing import Callable, Optional, Sequence

from duckdq.utils.dataframe import DataFrame


class NotColumnSpecifiedException(Exception):
    pass

class Precondition(ABC):
    pass

def find_first_failing(
    #  TODO: decide here if to send the full dataframe or the api
    engine,  # maybe dtypesx
    conditions: Sequence[Precondition],
) -> Optional[Exception]:

    try:
        engine.evaluate_preconditions(conditions)
    except Exception as e:
        return e

    return None

@dataclass
class HasColumn(Precondition):
    column: str

def has_column(column: str) -> Callable[[DataFrame], None]:
    return HasColumn(str)


@dataclass
class IsNumeric(Precondition):
    column: str

def is_numeric(column: str) -> Callable[[DataFrame], None]:
    return IsNumeric(str)

@dataclass
class AtLeastOne(Precondition):
    columns: Sequence[str]

def at_least_one(columns: Sequence[str]) -> Callable[[DataFrame], None]:
    return AtLeastOne(columns)

@dataclass
class IsString(Precondition):
    column: str

def is_string(column: str):
    return IsString(column)
