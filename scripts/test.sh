#!/bin/bash
# First create the TABLE we need
PGPASSWORD=password psql -U postgres -h 0.0.0.0 -d pgcdc -c "CREATE TABLE IF NOT EXISTS test_table(id serial primary key, name text);"
# Then loop every 3s and insert something to trigger a change in the WAL
x=1
while [ true ]
do
	name="W$x"
	# Insert into TABLE
	PGPASSWORD=password psql -U postgres -h 0.0.0.0 -d pgcdc -c "insert into test_table(name) values('$name');"
	echo "Inserted $name"
	x=$(( $x + 1 ))
	sleep 3
done