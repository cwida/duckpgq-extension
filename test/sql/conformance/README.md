# SQL/PGQ conformance tests

One sqllogictest file per feature group of ISO/IEC 9075-16:2023 Annex D, named after the feature IDs it
exercises. Every file states the clause its expectations are derived from and uses a small inline graph
so the expected result can be checked by hand. Error cases test the Syntax Rules of clauses 10.4 to 10.8.

Status per feature is tracked in `docs/SQLPGQ_CONFORMANCE.md`.
