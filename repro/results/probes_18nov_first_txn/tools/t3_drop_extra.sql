LOAD '/local/home/dwolde/bootstrap/duckpgq-extension/build/release/extension/duckpgq/duckpgq.duckdb_extension';
SET threads=32;
SET experimental_path_finding_operator=true;
.timer on
DROP INDEX pathfinding_benchmark_csr_rb0;
DROP INDEX pathfinding_benchmark_csr_rb2;
DROP INDEX pathfinding_benchmark_csr_rb8;
DROP INDEX pathfinding_benchmark_csr_rb64;
DROP INDEX pathfinding_benchmark_csr_undirected;
SELECT duckpgq_csr_index_stats('person_knows_person', 'pathfinding_benchmark_csr');
BEGIN TRANSACTION;
INSERT INTO person_knows_person SELECT person1id, person2id FROM knows_update_log WHERE seq > 0 AND seq <= 2000 AND op = 'insert';
DELETE FROM person_knows_person k USING (SELECT person1id, person2id FROM knows_update_log WHERE seq > 0 AND seq <= 2000 AND op = 'delete') u WHERE k.person1id = u.person1id AND k.person2id = u.person2id;
COMMIT;
