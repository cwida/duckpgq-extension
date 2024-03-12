import duckdb
import os
import pytest


# Get a fresh connection to DuckDB with the duckpgq extension binary loaded
@pytest.fixture
def duckdb_conn():
    extension_binary = os.getenv('DUCKPGQ_EXTENSION_BINARY_PATH')
    if extension_binary == '':
        raise Exception('Please make sure the `DUCKPGQ_EXTENSION_BINARY_PATH` is set to run the python tests')
    conn = duckdb.connect('', config={'allow_unsigned_extensions': 'true'})
    conn.execute(f"load '{extension_binary}'")
    return conn


def test_duckpgq(duckdb_conn):
    duckdb_conn.execute("SELECT duckpgq('Sam') as value;")
    res = duckdb_conn.fetchall()
    assert (res[0][0] == "Duckpgq Sam 🐥");


def test_property_graph(duckdb_conn):
    duckdb_conn.execute("CREATE TABLE foo(i bigint)")
    duckdb_conn.execute("INSERT INTO foo(i) VALUES (1)")
    duckdb_conn.execute("-CREATE PROPERTY GRAPH t VERTEX TABLES (foo);")
    duckdb_conn.execute("-FROM GRAPH_TABLE(t MATCH (f:foo))")
    res = duckdb_conn.fetchall()
    assert (res[0][0] == 1)
