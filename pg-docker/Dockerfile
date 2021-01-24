FROM postgres:13

RUN apt-get update \
	&& apt-get install postgresql-13-wal2json

LABEL maintainer="Speculare"

COPY pg.conf.sample /usr/share/postgresql/postgresql.conf.sample
COPY /docker-entrypoint-initdb.d /docker-entrypoint-initdb.d

EXPOSE 5432