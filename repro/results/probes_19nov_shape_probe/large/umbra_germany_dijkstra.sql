\set nav.buildindex n
create table e0 (src int, dest int, cost float);
copy e0 from '/home/dwolde/navindex/data/data/maps/germany.tsv';
create table e as select src, dest, cast(1 as float) as cost from e0;
create table q0 (src int, dest int);
copy q0 from '/home/dwolde/navindex/work_head/queries_germany.tsv';
create table q as select row_number() over (order by src, dest) - 1 as pos, src, dest from q0;
select 'MARK w2 1 0' as mark;
select count(l), coalesce(sum(l), 0) from (select array_length(shortest_path(table (select * from e), src, dest), 1) as l from q where pos < 1) x;
select 'MARK w2 10 0' as mark;
select count(l), coalesce(sum(l), 0) from (select array_length(shortest_path(table (select * from e), src, dest), 1) as l from q where pos < 10) x;
select 'MARK w2 100 0' as mark;
select count(l), coalesce(sum(l), 0) from (select array_length(shortest_path(table (select * from e), src, dest), 1) as l from q where pos < 100) x;
