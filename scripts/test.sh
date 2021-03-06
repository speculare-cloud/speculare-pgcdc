#!/bin/bash
# First create the TABLE we need
PGPASSWORD=password psql -U postgres -h 0.0.0.0 -d pgcdc -c "CREATE TABLE IF NOT EXISTS test_table0(id serial primary key, name text);"
PGPASSWORD=password psql -U postgres -h 0.0.0.0 -d pgcdc -c "CREATE TABLE IF NOT EXISTS test_table1(id serial primary key, name text);"
# Then loop every 3s and insert something to trigger a change in the WAL
x=1
while [ true ]
do
	name="W$x"
	y=$(($x%2))
	# Insert into TABLE
	PGPASSWORD=password psql -U postgres -h 0.0.0.0 -d pgcdc -c "insert into test_table$y(name) values('$name');"
	echo "Inserted $name"
	x=$(( $x + 1 ))
	sleep 1
	PGPASSWORD=password psql -U postgres -h 0.0.0.0 -d pgcdc -c "update test_table$y set name='~~$name' where name='$name';"
	sleep 1
done