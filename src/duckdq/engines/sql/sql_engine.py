import ctypes
import time
from dataclasses import dataclass
from typing import Set, List, Dict, FrozenSet, Sequence, Tuple
from abc import abstractmethod
import pandas as pd
import typing

from pandas import DataFrame
from sqlalchemy.engine import Connection
from sqlalchemy import inspect
from duckdb import DuckDBPyConnection

from duckdq.core.preconditions import Precondition, HasColumn, IsNumeric, IsString, AtLeastOne
from duckdq.core.properties import Property, Schema
from duckdq.core.metrics import Metric
from duckdq.engines.pandas import sketch_utils
from duckdq.utils.connection_handler import ConnectionHandler
from duckdq.utils.metrics_helper import metric_from_failure, metric_from_value
from duckdq.core.states import FrequenciesAndNumRows, SchemaState
from duckdq.engines.engine import Engine
from duckdq.engines.sql.sql_operators import SQLOperatorFactory
from duckdq.engines.sql.sql_operators import ScanShareableOperator, GroupingShareableOperator
from duckdq.metadata.metadata_repository import MetadataRepository, SQLAlchemyMetadataRepository, \
    DuckDBMetadataRepository, SQLMetadataRepository
from duckdq.utils.exceptions import UnknownOperatorTypeException, UnsupportedConnectionObjectException, \
    PreconditionNotMetException, UnsupportedPropertyException

@dataclass(frozen=True, eq=True)
class Grouping():
    grouping_cols: FrozenSet[str]
    num_rows_col: str
    filter: str

    def get_num_rows_aggregation(self) -> str:
        if not self.filter:
            count_agg = f"COUNT(*) as {self.num_rows_col}"
        else:
            count_agg = f"SUM(CASE WHEN {self.filter} THEN 1 ELSE 0 END) as {self.num_rows_col}"
        return count_agg

    def extract_num_rows(self, result: DataFrame) -> int:
        return int(result[self.num_rows_col][0])

    def identifier(self):
        return ctypes.c_size_t(self.__hash__()).value


class SQLEngine(Engine):
    NUMERIC_TYPES = {"NUMERIC", "TINYINT", "SMALLINT", "INTEGER", "BIGINT", "HUGEINT", "FLOAT", "DOUBLE", "REAL",
                     "DECIMAL"}
    STRING_TYPES = {"VARCHAR", "NVARCHAR", "CLOB", "TEXT"}

    def __init__(self, no_sharing=False):
        super().__init__()
        self.no_sharing = no_sharing
        self.schema = self.get_schema()

    def evaluate_preconditions(self, preconditions: Sequence[Precondition]):
        for cond in preconditions:
            if isinstance(cond, HasColumn) and cond.column not in self.schema:
                raise PreconditionNotMetException(f"Input data does not include column {cond.column}")
            elif isinstance(cond, IsNumeric) and self.schema[cond.column] not in SQLEngine.NUMERIC_TYPES:
                raise PreconditionNotMetException(f"Expected type of column {cond.column} to be"
                                                  f" one of {', '.join(SQLEngine.NUMERIC_TYPES)},"
                                                  f" but found {self.schema[cond.column]} instead!")
            elif isinstance(cond, IsString) and self.schema[cond.column] not in SQLEngine.STRING_TYPES:
                raise PreconditionNotMetException(f"Expected type of column {cond.column} to be"
                                                  f" one of {', '.join(SQLEngine.STRING_TYPES)},"
                                                  f" but found {self.schema[cond.column]} instead!")
            elif isinstance(cond, AtLeastOne) and len(cond.columns) < 1:
                raise PreconditionNotMetException("At least one column needs to be specified!")


    def compute_metrics(self, properties: Set[Property], repo: MetadataRepository) -> Dict[Property, Metric]:
        schema_property: Schema = None
        scanning_operators: List[ScanShareableOperator] = []
        grouping_operator_groups: Dict[Grouping, List[GroupingShareableOperator]] = {}
        metrics: Dict[Property, Metric] = {}

        for property in properties:
            if isinstance(property, Schema):
                schema_property = property
                continue

            try:
                operator = SQLOperatorFactory.create_operator(property)
            except UnsupportedPropertyException as ex:
                metrics[property] = metric_from_failure(ex, property)
                continue

            if isinstance(operator, ScanShareableOperator):
                scanning_operators.append(operator)
            elif isinstance(operator, GroupingShareableOperator):
                key = Grouping(operator.get_groupings(), operator.get_num_rows(), operator.filter)
                operators = grouping_operator_groups.setdefault(key,[])
                operators += [operator]
            else:
                raise UnknownOperatorTypeException(f"Operator '{operator.__class__.__name__}' "
                                                   f"is neither Scan nor Grouping operator.")

        if schema_property is not None:
            schema = self.get_schema()
            schema_state = SchemaState(schema_property.property_identifier(),schema)
            repo.register_state(schema_state)
            schema_metric = metric_from_value(
                schema, schema_property.name, schema_property.instance,schema_property.entity
            )
            metrics[schema_property] = schema_metric

        aggregations = set()
        # collect basic counts for grouping operators in first scan pass
        for grouping, grouping_operators in grouping_operator_groups.items():
            aggregations.add(grouping.get_num_rows_aggregation())

        if self.no_sharing:
            for operator in scanning_operators:
                aggregations = operator.get_aggregations()
                if len(aggregations) > 0:
                    scanning_result = self.execute_and_fetch(f"SELECT {', '.join(aggregations)} FROM {self.table}")
                    state = operator.extract_state(scanning_result)
                    state = repo.register_state(state)
                    metric = operator.get_metric(state)
                    metrics[operator.get_property()] = metric
        else:
            for operator in scanning_operators:
                aggregations = aggregations.union(operator.get_aggregations())

            if len(aggregations) > 0:
                scanning_result = self.execute_and_fetch(f"SELECT {', '.join(aggregations)} FROM {self.table}")

            for operator in scanning_operators:
                state = operator.extract_state(scanning_result)
                state = repo.register_state(state)
                metric = operator.get_metric(state)
                metrics[operator.get_property()] = metric

        for grouping, grouping_operators in grouping_operator_groups.items():
            grouping_columns = list(grouping.grouping_cols)
            num_rows = grouping.extract_num_rows(scanning_result)
            if grouping.filter is None:
                query = f"SELECT {', '.join(list(grouping_columns) + ['COUNT(*) as count'])} FROM {self.table} GROUP BY {', '.join(grouping_columns)}"
            else:
                query = f"SELECT {', '.join(list(grouping_columns) + ['COUNT(*) as count'])} FROM {self.table} WHERE {grouping.filter} GROUP BY {', '.join(grouping_columns)}"

            frequencies_table = repo.get_frequency_table_name(grouping.identifier())
            if self.is_local_state_handler(repo):
                self.execute_and_store(query, frequencies_table)
                state = FrequenciesAndNumRows(grouping.identifier(), frequencies_table, grouping_columns, num_rows)
            else:
                self.execute_and_store(query, frequencies_table, temp=True)
                grouping_df = self.execute_and_fetch(f"SELECT * FROM {frequencies_table}")
                state = FrequenciesAndNumRows(grouping.identifier(), frequencies_table, grouping_columns, num_rows)
                state.set_df(grouping_df)

            aggregations = set()
            for operator in grouping_operators:
                aggregations = aggregations.union(operator.get_aggregations())

            grouping_result = self.execute_and_fetch(f"SELECT {', '.join(aggregations)} FROM {frequencies_table}")
            for operator in grouping_operators:
                metric = operator.extract_metric(grouping_result, num_rows)
                metrics[operator.get_property()] = metric
                state.id = operator.get_property().property_identifier()
                state = repo.register_state(state)

        return metrics

    @abstractmethod
    def get_schema(self) -> Dict[str, str]:
        pass

    @abstractmethod
    def execute_and_fetch(self, query: str) -> DataFrame:
        pass

    def execute_and_store(self, query: str, table: str, temp: bool = False) -> str:
        self.con.execute(f"DROP TABLE IF EXISTS {table}")
        self.con.execute(f"CREATE {'TEMP ' if temp else ''}TABLE {table} AS {query}")
        return table

    @abstractmethod
    def is_local_state_handler(self, repo: MetadataRepository):
        pass


class DuckDBEngine(SQLEngine):
    def __init__(self, con: DuckDBPyConnection, table, no_sharing = False):
        self.con = con
        self.table = table
        super().__init__(no_sharing)

    def profile(self, with_quantiles=True):
        table_info = self.con.execute(f"PRAGMA table_info({self.table})").fetchdf()
        names = list(table_info["name"])
        types = list(table_info["type"])
        schema = dict(zip(names,types))

        #stats_select = ', '.join([f"stats({col})" for col in list(table_info["name"])])
        #stats_list = list(self.con.execute(f"SELECT {stats_select} FROM {self.table} LIMIT 1").fetchdf().iloc[0])
        #stats_dict = {}
        #for i, stats in enumerate(stats_list):
        #    start, end = (stats.find('[')+1, stats.find(']'))
        #    stats_dict[names[i]] = stats[start:end].split(",")
        stats_selects = []
        stats_selects.append("COUNT(*)")
        for col, type in schema.items():
            if type in ["INTEGER","BIGINT","DOUBLE","FLOAT"]:
                #, 0.05: {{}}, Q1: {{}}, Median: {{}}, Q3: {{}}, 0.95: {{}}
                select = f"format('Missing: {{}}, Min: {{}}, Max: {{}}, Mean: {{}}', " \
                         f"SUM(CASE WHEN {col} IS NULL THEN 1 ELSE 0 END), " \
                         f"min({col}), " \
                         f"max({col}), " \
                         f"round(avg({col}),2))"
                #, " \
                #f"quantile({col},0.05), " \
                #f"quantile({col},0.25), " \
                #f"quantile({col},0.5), " \
                #f"quantile({col},0.75), " \
                #f"quantile({col},0.95))
            else:
                select = f"format('Missing: {{}}, Min: {{}}, Max: {{}}, Min Length: {{}}, Max Length: {{}}', " \
                         f"SUM(CASE WHEN {col} IS NULL THEN 1 ELSE 0 END), " \
                         f"min({col}), " \
                         f"max({col}), " \
                         f"min(strlen({col})), " \
                         f"max(strlen({col})) )"
            stats_selects.append(select)
        stats_select = ', '.join(stats_selects)
        stats_list = list(self.con.execute(f"SELECT {stats_select} FROM {self.table} LIMIT 1").fetchdf().iloc[0])
        count = int(stats_list[0])
        stats_list = stats_list[1:]
        stats_dict = {}
        for i, stats in enumerate(stats_list):
            stat_arr = stats.split(", ")
            stat_pairs = []
            for stat in stat_arr:
                stat_pairs.append(stat.split(": "))
            stats_dict[names[i]] = stat_pairs
        samples = self.con.execute(f"SELECT {', '.join(names)} FROM {self.table} USING SAMPLE 5").fetchdf()
        samples_dict = samples.to_dict()

        if with_quantiles:
            start = time.time()
            np = self.con.execute(f"SELECT * FROM {self.table}").fetchnumpy()
            end = time.time()
            print(f"{end-start}")
            sketch_statistics = sketch_utils.calculate_sketch_statistics_np(np)

            for col, stat in stats_dict.items():
                if col in sketch_statistics.keys():
                    stat += sketch_statistics[col]

        return {"count": count, "schema": schema, "stats": stats_dict, "samples": samples_dict}

    def get_schema(self) -> Dict[str, str]:
        df = self.con.execute(f"PRAGMA table_info('{self.table}')").fetchdf()
        schema = {}
        for index, col in df.iterrows():
            schema[col["name"]] = col["type"]
        return schema

    def execute_and_fetch(self, query: str) -> DataFrame:
        return self.con.execute(query).fetchdf()

    def is_local_state_handler(self, repo: MetadataRepository):
        if isinstance(repo, DuckDBMetadataRepository):
            duckdb_state_handler: DuckDBEngine = typing.cast(DuckDBMetadataRepository, repo)
            return duckdb_state_handler.con == self.con
        else:
            return False


class SQLAlchemyEngine(SQLEngine):
    def __init__(self, con: Connection, table):
        self.con = con
        self.table = table
        super().__init__()

    def profile(self):
        raise NotImplementedError("Profiling for SQLAlchemyEngine not implemented, yet.")

    def get_schema(self) -> Dict[str, str]:
        inspector = inspect(self.con)
        res = inspector.get_columns(self.table)
        schema = {}
        for col in res:
            schema[col["name"]] = col["type"]
        return schema

    def execute_and_fetch(self, query: str) -> DataFrame:
        rs = self.con.execute(query)
        df = pd.DataFrame(rs.fetchall())
        df.columns = rs.keys()
        return df

    def is_local_state_handler(self, repo: MetadataRepository):
        if isinstance(repo, SQLAlchemyMetadataRepository):
            sqlalchemy_repo: SQLAlchemyEngine = typing.cast(SQLAlchemyMetadataRepository, repo)
            return sqlalchemy_repo.con == self.con
        else:
            return False


class SQLEngineFactory():
    @classmethod
    def create_sql_engine(cls, url_or_con, table):
        con = ConnectionHandler.get_connection(url_or_con)
        if isinstance(con, DuckDBPyConnection):
            return DuckDBEngine(con, table)
        elif isinstance(con, Connection):
            return SQLAlchemyEngine(con, table)
        else:
            raise UnsupportedConnectionObjectException(f"Connection object '{con.__class__.__name__}' not supported.")
