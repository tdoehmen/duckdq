import duckdb
from duckdb import DuckDBPyConnection
from sqlalchemy.engine import Connection, create_engine

from duckdq.utils.exceptions import UnsupportedConnectionObjectException


class ConnectionHandler:
    external_connections = {}
    handled_connections = {}

    @classmethod
    def get_connection(cls, url_or_con):
        if isinstance(url_or_con, DuckDBPyConnection) or isinstance(url_or_con, Connection):
            idd = id(url_or_con)
            con = url_or_con
            cls.external_connections[idd] = con
        elif isinstance(url_or_con, str):
            url = url_or_con
            if "//" not in url: #then, assume it's a duckdb connection
                url = "duckdb://"+url

            if url in cls.handled_connections:
                con = cls.handled_connections[url]
            elif url.startswith("duckdb://"):
                db_name = url.replace("duckdb://", "", 1)
                con = duckdb.connect(db_name)
                cls.handled_connections[url] = con
            else:
                db_engine = create_engine(url)
                con = db_engine.connect()
                cls.handled_connections[url] = con
        else:
            raise UnsupportedConnectionObjectException(f"Connection object '{url_or_con.__class__.__name__}' not supported.")

        return con

    @classmethod
    def close_connections(cls):
        for url, con in cls.handled_connections.items():
            if url.startswith("duckdb://"):
                db_name = url.replace("duckdb://", "", 1)

                con.execute("CREATE TABLE dummy AS SELECT * FROM range(200000)")
                con.execute("DROP TABLE dummy")

                con.close()

                con = duckdb.connect(db_name)
                con.close()
            else:
                con.close()
        cls.handled_connections.clear()
