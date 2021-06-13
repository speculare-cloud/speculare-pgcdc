FROM postgres:13

RUN apt-get update \
	&& apt-get install -y postgresql-13-wal2json postgresql-13-partman

LABEL maintainer="Speculare"

COPY pg.conf.sample /usr/share/postgresql/postgresql.conf.sample
COPY /docker-entrypoint-initdb.d /docker-entrypoint-initdb.d

EXPOSE 5432