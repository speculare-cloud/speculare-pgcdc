<div align="center">
  <h1>Speculare PGCDC</h1>
  <p>
    <strong>Capture Data Change for Speculare</strong>
  </p>
  <p>

[![Apache 2 License](https://img.shields.io/badge/license-Apache%202-blue.svg)](LICENSE)
[![CI](https://github.com/speculare-cloud/speculare-pgcdc/workflows/CI/badge.svg)](https://github.com/speculare-cloud/speculare-pgcdc/actions)
[![Docs](https://img.shields.io/badge/Docs-latest-green.svg)](https://docs.speculare.cloud)

  </p>
</div>

Speculare PGCDC allows you to listen to changes in your PostgreSQL database via logical replication and then broadcast those changes over websockets.

`Realtime` server works by:
1. listening to PostgreSQL's logical replication (here using Wal2Json).
2. filtering the incoming message
3. broadcasting the message over websocket

Explaination
--------------------------

You probably know that Postgresql is not a realtime databse. So if we want to stream the change of it to a websocket or any UI it's not possible by default.

Hopefully Postgresql have that sweet feature named `Logical Replication`, which stream the change made into the database over a replication slot. We can use a multitude of plugins to format the output of this stream, but for Speculare-PGCDC we've chosen to use wal2json.

This project create a replication slot on the targeted postgres instance and then stream the change from this slot to all the websockets connected.

Project Status
--------------------------

This project is still under development and the documentation is constantly changing. 
You are welcome to try it, but expect some changes with future updates. 
TAGs are used to mark a certain version in all Speculare repositories as compatible with each others.

Server setup / Dev setup
--------------------------

- Install all deps
```bash
$ curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
$ sudo apt-get install libssl-dev libpq-dev pkg-config build-essential
```

- Create a pgcdc.config file based on pgcdc.example.config

> **⚠ WARNING: Check the [docs](https://docs.speculare.cloud) !**

Usage
--------------------------

```
$ wss://server/ws?query=change_type:table:col.eq.val
```
will get `change_type` event from `table` where `col` is `equals` to `val`.

The `change_type` and `table` parameters are mandatory, if you're missing them you'll get a 400 error.
`change_type` can be any of those: *, insert, update, delete.
`table` must be a valid table of your database.

I decided to restrict the API in such way that a single websocket can only listen to one table. 
This might change in the future if needed, but as of now and in the current shape of Speculare, it's not needed.

Contributing
--------------------------

Pull requests are welcome. For major changes, please open an issue first to discuss what you would like to change.