var duckdb = require('../../duckdb-pgq/tools/nodejs');
var assert = require('assert');

describe(`sqlpgq extension`, () => {
    let db;
    let conn;
    before((done) => {
        db = new duckdb.Database(':memory:');
        conn = new duckdb.Connection(db);
        done();
    });

    it('function should return expected constant', function (done) {
        db.all("SELECT sqlpgq('Sam') as value;", function (err, res) {
            if (err) throw err;
            assert.deepEqual(res, [{value: "Sqlpgq Sam 🐥"}]);
            done();
        });
    });
});