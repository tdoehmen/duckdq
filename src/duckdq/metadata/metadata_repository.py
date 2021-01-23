import dataclasses
import hashlib
import uuid
import json
import sys
import dill
from datetime import datetime
from abc import ABCMeta, abstractmethod

from sqlalchemy.engine import Connection
from duckdb import DuckDBPyConnection
from typing import Dict

from duckdq.core.metrics import Metric
from duckdq.core.properties import Property
from duckdq.core.states import *
from duckdq.utils.connection_handler import ConnectionHandler
from duckdq.utils.exceptions import UnsupportedConnectionObjectException, StateHandlerUnsupportedStateException


class MetadataRepository(metaclass=ABCMeta):

    def set_dataset(self, dataset_id: str = None, partition_id: str = None):
        self.timestamp = datetime.now()
        self.run_id = str(uuid.uuid4()).replace("-","_")
        if dataset_id is None:
            self.dataset_id = str(uuid.uuid4())
        else:
            self.dataset_id = dataset_id

        if partition_id is None:
            self.partition_id = str(uuid.uuid4())
        else:
            self.partition_id = partition_id

    @abstractmethod
    def add_profile(self, profile: Dict):
        pass

    @abstractmethod
    def store_checks(self, checks: List) -> str:
        pass

    @abstractmethod
    def store_metrics(self, metrics: Dict[Property, Metric]):
        pass

    @abstractmethod
    def register_state(self, state: State) -> State:
        pass

    def get_frequency_table_name(self, identifier):
        s = self.run_id + str(identifier)
        id = int(hashlib.sha1(s.encode("utf-8")).hexdigest(), 16) % (10 ** 16)
        return f"{SQLMetadataRepository.state_table_prefix}_{id}"


class InMemoryMetadataRepository(MetadataRepository):
    def __init__(self):
        self.state_map = {}
        self.metric_map = {}
        super().set_dataset()

    def register_state(self, state: State) -> State:
        state_id = state.id
        self.state_map[state_id] = state
        return state

    def add_profile(self, profile: Dict):
        return None

    def store_checks(self, checks) -> str:
        return str(uuid.uuid4())

    def store_metrics(self, metrics: Dict[Property, Metric]):
        return None

    def register_state(self, state: State) -> State:
        return state

class SQLMetadataRepository(MetadataRepository):
    metrics_table = "dq_metrics" #run_id, check_id, metric_id, property, metric_type, metric_value
    check_table = "dq_checks" #run_id, check_id, check_serialized
    validation_runs = "dq_validation_runs" #run_id, timestamp, dataset_id, partition_id
    states_table = "dq_states" #run_id, state_id, property, state_type, state_serialized
    state_table_prefix = "dq_state_freq" #{_state_id} term, count, merged

    def __init__(self, con):
        self.con = con
        self.__create_tables__()
        super().set_dataset()

    def __create_tables__(self):
        self.execute(f"CREATE TABLE IF NOT EXISTS {self.metrics_table}(run_id VARCHAR, property_id VARCHAR, metric_type VARCHAR, metric_value VARCHAR)")
        self.execute(f"CREATE TABLE IF NOT EXISTS {self.check_table}(run_id VARCHAR, check_description VARCHAR, check_level VARCHAR, check_serialized VARCHAR)")
        self.execute(f"CREATE TABLE IF NOT EXISTS {self.validation_runs}(run_id VARCHAR, ts TIMESTAMP, dataset_id VARCHAR, partition_id VARCHAR)")
        self.execute(f"CREATE TABLE IF NOT EXISTS {self.states_table}(run_id VARCHAR, property_id VARCHAR, state_type VARCHAR, state_serialized VARCHAR)")

    def set_dataset(self, dataset_id: str = None, partition_id: str = None):
        super().set_dataset(dataset_id,partition_id)
        self.__log_run__()

    def __log_run__(self):
        self.execute(f"INSERT INTO {self.validation_runs} VALUES('{self.run_id}', '{self.timestamp}', '{self.dataset_id}', '{self.partition_id}')")

    def register_state(self, state: State) -> State:
        state_id = state.id
        if isinstance(state, SerializableState):
            state_json = json.dumps(dataclasses.asdict(state))
            self.execute(f"INSERT INTO {self.states_table} VALUES ('{self.run_id}', '{state_id}', '{state.__class__.__name__}', '{state_json}')")
        elif isinstance(state, TabularState):
            if state.has_df:
                self.store_table(state.df, state.get_table_name())
            state_json = json.dumps(dataclasses.asdict(state))
            self.execute(f"INSERT INTO {self.states_table} VALUES ('{self.run_id}', '{state_id}', '{state.__class__.__name__}', '{state_json}')")
        else:
            raise StateHandlerUnsupportedStateException(f"State type '{state.__class__.__name__}' not supported by SQLStateHandler for registration.")

        #if self.merge_states:
        #    state = self.merge_state(state_id)
        return state

    #def merge_state(self, state_id: str) -> State:
    #    current_state = self.get_state(state_id)
    #    return current_state

    def get_state(self, state_id: str) -> State:
        state_row = self.execute_and_fetch(f"SELECT state_type, state_serialized FROM {self.states_table} WHERE run_id='{self.run_id}' AND state_id='{state_id}'")
        state_type = state_row["state_type"][0]
        state_serialized = state_row["state_serialized"][0]
        try:
            state_class = getattr(sys.modules[__name__], state_type)
        except AttributeError:
            raise StateHandlerUnsupportedStateException(f"State type '{state_type}' not supported by SQLStateHandler for loading.")

        if issubclass(state_class, SerializableState) or issubclass(state_class, TabularState):
            json_dict = json.loads(state_serialized)
            state = state_class(**json_dict)
        else:
            raise StateHandlerUnsupportedStateException(f"State type '{state_type}' not supported by SQLStateHandler for loading.")

        return state

    def add_profile(self, profile: Dict):
        return None

    def store_checks(self, checks):
        for check in checks:
            check_description = check.description
            check_level = check.level.name
            check_serialized = dill.dumps(check).hex()
            self.execute(f"INSERT INTO {self.check_table} VALUES ('{self.run_id}', '{check_description}', '{check_level}', '{check_serialized}')")


    def store_metrics(self, metrics: Dict[Property, Metric]):
        for property, metric in metrics.items():
            metric_type = metric.__class__.__name__
            metric_str = dill.dumps(metric).hex()
            self.execute(f"INSERT INTO {self.metrics_table} VALUES ('{self.run_id}', '{property.property_identifier()}', '{metric_type}', '{metric_str}')")


    @abstractmethod
    def execute(self, query: str):
        pass

    @abstractmethod
    def execute_and_fetch(self, query: str) -> DataFrame:
        pass

    @abstractmethod
    def store_table(self, df: DataFrame, table_name: str):
        pass

class SQLAlchemyMetadataRepository(SQLMetadataRepository):

    def execute(self, query: str):
        self.con.execute(query)

    def execute_and_fetch(self, query: str) -> DataFrame:
        rs = self.con.execute(query)
        df = DataFrame(rs.fetchall())
        df.columns = rs.keys()
        return df

    def store_table(self, df: DataFrame, table_name: str):
        df.to_sql(table_name, self.con, index=False)

class DuckDBMetadataRepository(SQLMetadataRepository):

    def execute(self, query: str):
        self.con.execute(query)

    def execute_and_fetch(self, query: str) -> DataFrame:
        return self.con.execute(query).fetchdf()

    def store_table(self, df: DataFrame, table_name: str):
        self.con.from_df(df).create(table_name)

class SQLMetadataRepositoryFactory():

    @classmethod
    def create_sql_metadata_repository(cls, url_or_con) -> SQLMetadataRepository:
        con = ConnectionHandler.get_connection(url_or_con)
        if isinstance(con, DuckDBPyConnection):
            return DuckDBMetadataRepository(con)
        elif isinstance(con, Connection):
            return SQLAlchemyMetadataRepository(con)
        else:
            raise UnsupportedConnectionObjectException(f"Connection object {con} not supported.")
