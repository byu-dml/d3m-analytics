# Programmatically Query The D3M MtL Database 

Use this package to programmatically query the D3M meta-learning database. It uses the `elasticsearch-dsl` python client to query.

## Getting Started

### Installation:

1.  ```shell
    pip install -r requirements.txt
    ```

1.  Add an `.env` file to root, and include these values:

    ```env
    CLIENT=<db_access_username>
    SECRET=<db_access_password>
    API=<db_access_url>
    ```

### Basic Usage

```shell
python src/main.py
```