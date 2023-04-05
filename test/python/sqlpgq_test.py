import duckdb

def test_sqlpgq():
    conn = duckdb.connect('');
    conn.execute("SELECT sqlpgq('Sam') as value;");
    res = conn.fetchall()
    assert(res[0][0] == "Sqlpgq Sam 🐥");