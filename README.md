<p align="center">
  <img src="https://user-images.githubusercontent.com/172204/193307982-a286c574-80ef-41de-b52f-1b064ae7fccd.png" alt="Databend logo" width="300"/>
  <img src="https://raw.githubusercontent.com/dbt-labs/dbt/ec7dee39f793aa4f7dd3dae37282cc87664813e4/etc/dbt-logo-full.svg" alt="dbt logo" width="250"/>
</p>

# dbt-databend-cloud

![PyPI](https://img.shields.io/pypi/v/dbt-databend-cloud)
![PyPI - Python Version](https://img.shields.io/pypi/pyversions/dbt-databend-cloud)

The `dbt-databend-cloud` package contains all of the code enabling [dbt](https://getdbt.com) to work with
[Databend Cloud](https://databend.rs/doc/cloud/).

## Table of Contents
* [Installation](#installation)
* [Supported features](#supported-features)
* [Profile Configuration](#profile-configuration)
* [Database User Privileges](#database-user-privileges)
* [Running Tests](#running-tests)
* [Example](#example)
* [Contributing](#contributing)

## Installation
Compile by source code.

```bash
$ git clone https://github.com/databendcloud/dbt-databend.git
$ cd dbt-databend
$ pip install .
```
Also, you can get it from pypi.

```bash
$ pip install dbt-databend-cloud
```
## Supported features

 | ok |           Feature           |
|:--:|:---------------------------:|
|  ✅ |    Table materialization    |
|  ✅ |    View materialization     |
|  ✅ | Incremental materialization |
|  ❌  |  Ephemeral materialization  |
|  ✅ |            Seeds            |
|  ✅ |           Sources           |
|  ✅ |      Custom data tests      |
|  ✅ |        Docs generate        |
|  ❌ |          Snapshots          |
|  ✅ |      Connection retry       |

Note:

* Databend does not support `Ephemeral` and `SnapShot`. You can find more detail [here](https://github.com/datafuselabs/databend/issues/8685)

## Profile Configuration

Databend Cloud targets should be set up using the following configuration in your `profiles.yml` file.

**Example entry for profiles.yml:**

```
Your_Profile_Name:
  target: dev
  outputs:
    dev:
      type: databend
      host: [host]
      port: [port]
      schema: [schema(Your database)]
      user: [username]
      pass: [password]
```

| Option   | Description                                           | Required? | Example                        |
|----------|-------------------------------------------------------|-----------|--------------------------------|
| type     | The specific adapter to use                           | Required  | `databend`                     |
| host     | The server (hostname) to connect to                   | Required  | `yourorg.databend.com`         |
| port     | The port to use                                       | Required  | `443`                          |
| schema   | Specify the schema (database) to build models into    | Required  | `analytics`                    |
| user     | The username to use to connect to the server          | Required  | `dbt_admin`                    |
| pass     | The password to use for authenticating to the server  | Required  | `correct-horse-battery-staple` |


Note:

* You can find your host, user, pass information in this [docs](https://docs.databend.com/using-databend-cloud/warehouses/connecting-a-warehouse)

## Running Tests

See [tests/README.md](tests/README.md) for details on running the integration tests.

## Example

Click [here](https://github.com/databendcloud/dbt-databend/wiki/How-to-use-dbt-with-Databend-Cloud) to see a simple example about using dbt with dbt-databend-cloud.

## Contributing

Welcome to contribute for dbt-databend-cloud. See [Contributing Guide](CONTRIBUTING.md) for more information.
