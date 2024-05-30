#!/usr/bin/env python
from setuptools import find_namespace_packages, setup

package_name = "dbt-databend-cloud"
# make sure this always matches dbt/adapters/{adapter}/__version__.py
package_version = "1.8.1"
description = """The Databend adapter plugin for dbt"""

setup(
    name=package_name,
    version=package_version,
    description=description,
    long_description=description,
    author="Databend Cloud Team",
    author_email="zhangzhihan@datafuselabs.com",
    url="https://github.com/databendcloud/dbt-databend.git",
    packages=find_namespace_packages(include=["dbt", "dbt.*"]),
    include_package_data=True,
    install_requires=[
        "dbt-common>=0.1.0a1,<2.0",
        "dbt-adapters>=0.1.0a1,<2.0",
        "databend-sqlalchemy~=0.3.2",
        "agate",
    ],
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: Microsoft :: Windows",
        "Operating System :: MacOS :: MacOS X",
        "Operating System :: POSIX :: Linux",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.1.x",
    ],
    python_requires=">=3.8",
)
