#!/usr/bin/env python
from setuptools import find_namespace_packages, setup

package_name = "dbt-databend"
# make sure this always matches dbt/adapters/{adapter}/__version__.py
package_version = "1.3.0"
description = """The Databend adapter plugin for dbt"""

setup(
    name=package_name,
    version=package_version,
    description=description,
    long_description=description,
    author="zhihanz",
    author_email="zhangzhihan@datafuselabs.com",
    url="https://github.com/ZhiHanZ/dbt-databend.git",
    packages=find_namespace_packages(include=["dbt", "dbt.*"]),
    include_package_data=True,
    install_requires=[
        "dbt-core~=1.3.0.",
    ],
)
