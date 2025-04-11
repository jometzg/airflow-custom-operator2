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

## Contributing

Contributions are welcome! Please submit a pull request or open an issue for any enhancements or bug fixes.

## License

This project is licensed under the MIT License. See the LICENSE file for details.
