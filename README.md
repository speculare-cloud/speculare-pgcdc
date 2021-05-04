<div align="center">
  <h1>Speculare PGCDC</h1>
  <p>
    <strong>Capture Data Change for Speculare</strong>
  </p>
  <p>

[![Apache 2 License](https://img.shields.io/badge/license-Apache%202-blue.svg)](LICENSE)

  </p>
</div>

Speculare PGCDC is intended to stream the change from the Posgtresql database using CDC.

This project is meant to evolve in something more complete and more complexe in a somewhat near future.


Explaination
--------------------------

As you probably know, Postgresql is not a realtime databse. So if we want to stream the change of it to a websocket or any UI it's not possible by default.

Hopefully Postgresql have that sweet feature named `Logical Replication`, which stream the change made into the database over a replication slot. We can use a multitude of plugin to format the output of this stream, but for Speculare-PGCDC I've chosen to use wal2json.

This project "simply" create a replication slot on the targeted postgres instance (if it's supported) and then stream the change from this slot to all the websockets connected. It offer you some benefit such as only getting change from one table, or for one type of event `INSERT, UPDATE, DELETE` or even by filtering results like you would do with a `WHERE` clause in SQL (simpler, only one WHERE clause).

Server setup / Dev setup
--------------------------

- Install all deps
```bash
$ curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
$ sudo apt-get install libssl-dev libpq-dev pkg-config build-essential
```

There pretty much only one step in this setup (other than Docker (see below))

- Create a .env file based on .env.example

Usage
--------------------------

```
$ curl /ws?query=change_type:table:col.eq.val
```
_will get change_type event from table where col is equals to val

The `change_table` and `table` parameters are mandatory, if you're missing them you'll get a 400 error.
Change_table can be either of those: *, insert, update, delete.
Table must be a valid table of your database.

I decided to restrict the API in such way that a single websocket can only listen to one table. This might change in the future if needed, but as of now and in the current shape of Speculare, it's not needed.

Setup Docker
--------------------------

In order to make developing on this project easier, I've made a Docker image of the Postgres server with the appropriate config as well as some testing scripts. You can use them by following the instructions below.

If you don't have Docker installed yet, you can try to install it by running:
```bash
$ ./scripts/installDocker.sh
```

You first need to build the image of the database container:
```bash
$ ./scripts/startPgDocker.sh
```

Then you can run the following script that will create the test tables and INSERT / UPDATE data inside the database to trigger changes:
```bash
$ ./scripts/test.sh
```

Contributing
--------------------------

Pull requests are welcome. For major changes, please open an issue first to discuss what you would like to change.