import duckdb

def test_duckpgq():
    conn = duckdb.connect('')
    conn.execute("SELECT duckpgq('Sam') as value;")
    res = conn.fetchall()
    assert(res[0][0] == "Duckpgq Sam 🐥")