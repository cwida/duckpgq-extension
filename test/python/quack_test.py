import duckdb

def test_quack():
    conn = duckdb.connect('');
    conn.execute("SELECT quack('Sam') as value;");
    res = conn.fetchall()
    assert(res[0][0] == "Quack Sam 🐥");