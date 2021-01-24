#!/bin/bash
docker build pg-docker -t pgcdc
docker run -p 5432:5432 -e POSTGRES_DB=pgcdc -e POSTGRES_PASSWORD=password pgcdc