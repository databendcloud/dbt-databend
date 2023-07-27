# Testing dbt-databend-cloud

## Overview

Here are the steps to run the tests:
1. Set up
2. Get config
3. Run tests

## Set up

Make sure you have python environment, you can find the supported python version in setup.py.
```bash
pip3 install -r requirements_dev.txt
pip3 install .
```

## Get config
Config the configurations in `conftest.py`:

```python
{
        "type": "databend",
        "host": "host",
        "port": 443,
        "user": "user",
        "pass": "pass",
        "schema": "your database",
        "secure": True,
    }
```

You can get the config information by the way in this [docs](https://docs.databend.com/using-databend-cloud/warehouses/connecting-a-warehouse).

## Run tests

```shell
python -m pytest -s -vv   tests/functional
```
