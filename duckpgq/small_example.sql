force install '/Users/dljtw/git/duckpgq/build/debug/extension/duckpgq/duckpgq.duckdb_extension';

load '/Users/dljtw/git/duckpgq/build/debug/extension/duckpgq/duckpgq.duckdb_extension';

CREATE TABLE Student(id BIGINT, name VARCHAR);

CREATE TABLE know(src BIGINT, dst BIGINT, createDate BIGINT);

CREATE TABLE School(name VARCHAR, Id BIGINT, Kind VARCHAR);

CREATE TABLE StudyAt(personId BIGINT, schoolId BIGINT);

INSERT INTO Student VALUES (0, 'Daniel'), (1, 'Tavneet'), (2, 'Gabor'), (3, 'Peter'), (4, 'David');

INSERT INTO know VALUES (0,1, 10), (0,2, 11), (0,3, 12), (3,0, 13), (1,2, 14), (1,3, 15), (2,3, 16), (4,3, 17);

INSERT INTO School VALUES ('VU', 0, 'University'), ('UVA', 1, 'University');

INSERT INTO StudyAt VALUES (0, 0), (1, 0), (2, 1), (3, 1), (4, 1);
