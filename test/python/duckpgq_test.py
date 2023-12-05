import duckdb
import os
import pytest

# Get a fresh connection to DuckDB with the duckpgq extension binary loaded
@pytest.fixture
def duckdb_conn():
    extension_binary = os.getenv('DUCKPGQ_EXTENSION_BINARY_PATH')
    if (extension_binary == ''):
        raise Exception('Please make sure the `DUCKPGQ_EXTENSION_BINARY_PATH` is set to run the python tests')
    conn = duckdb.connect('', config={'allow_unsigned_extensions': 'true'})
    conn.execute(f"load '{extension_binary}'")
    return conn

def test_duckpgq(duckdb_conn):
    duckdb_conn.execute("SELECT duckpgq('Sam') as value;");
    res = duckdb_conn.fetchall()
    assert(res[0][0] == "Duckpgq Sam 🐥");

def test_duckpgq_openssl_version_test(duckdb_conn):
    duckdb_conn.execute("SELECT duckpgq_openssl_version('Michael');");
    res = duckdb_conn.fetchall()
    assert(res[0][0][0:51] == "Duckpgq Michael, my linked OpenSSL version is OpenSSL");