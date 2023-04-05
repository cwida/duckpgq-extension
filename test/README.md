# Testing the quack extension
This directory contains all the tests for the quack extension. The `sql` directory holds tests that are written as [SQLLogicTests](https://duckdb.org/dev/sqllogictest/intro.html). DuckDB aims to have most its tests in this format as SQL statements, so for the quack extension, this should probably be the goal too. However, client specific testing is also available.

The root makefile contains targets to build and run all of these tests. To run the SQLLogicTests:
```bash
make test
```

To run the python tests:
```sql
make test_python
```

For other client tests check the makefile in the root of this repository.