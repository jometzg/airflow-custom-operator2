# Airflow Custom Operator

This project provides a custom Apache Airflow operator, `MyCustomOperator`, designed to extend the functionality of Airflow workflows. 

## Overview

The `MyCustomOperator` allows users to define custom tasks within their Airflow DAGs, enabling more complex workflows tailored to specific needs.

## Installation

To install the custom operator, clone the repository and install the required dependencies:

```bash
git clone <repository-url>
cd airflow-custom-operator
pip install -r requirements.txt
```

## Usage

To use the `MyCustomOperator` in your Airflow DAG, you can import it as follows:

```python
from datetime import datetime
from airflow import DAG

 # Import from private package
from custom_operator.my_custom_operator import MyCustomOperator

# test dag
with DAG(
"test-custom-package",
tags=["example"],
description="A simple tutorial DAG",
schedule_interval=None,
start_date=datetime(2025, 1, 1),
) as dag:
    task = MyCustomOperator(task_id="sample-task",  param1="hello", param2="world",)

    task
```

## Testing

Unit tests for the `MyCustomOperator` are located in the `tests` directory. You can run the tests using:

```bash
pytest tests/
```

## Building for the Package for Distribution
The package needs to be built and it is this package that the Airflow DAGs use:

```bash
python setup.py sdist bdist_wheel
```

## Use Inside Apache Airflow in Microsoft Fabric
To use this inside a Microsoft Fabric hosted Apache Airflow session, two things need to be done:

1. The source for the DAGs needs to be GitHub hosted.
2. The path to the packaged Operator needs to be added to the Apache Airflow requirements.

### Using a GitHub Repository
The source for all code for Airflow needs to point to a source code source. As this repository is in GitHub, this needs to be set. The Airflow configuration is read-only, so if the repository is public-faced, then no credentials are needed. For private repositories, credentials will be needed.
![alt text](./images/airflow-git.png "Airflow GitHub Settings")

### Apache Airflow requirements
This is configured in the environment variables section and a path the the packaged version of the Operator needs to be added.
This follows a specific pattern as described in [Install a Private Package as a requirement in Apache Airflow job](https://learn.microsoft.com/en-us/fabric/data-factory/apache-airflow-jobs-install-private-package)
![alt text](./images/airflow-requirements.png "Airflow requirements")

## License

This project is licensed under the MIT License. See the LICENSE file for details.
