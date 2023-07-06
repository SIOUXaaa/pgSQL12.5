CREATE EXTENSION pg_dirtyread;
CREATE TABLE foo (a int, b int);
ALTER TABLE foo SET (
    autovacuum_enabled = false, toast.autovacuum_enabled = false
);
INSERT INTO foo VALUES (1, 1), (2, 2);
DELETE FROM foo WHERE a = 1;
select * from foo;
select * from dirtyread('foo') as t(a int, b int);
DROP TABLE foo;
DROP EXTENSION pg_dirtyread;