# Programmatically Interact With The D3M MtL Database 

You can use this package to programmatically interact with the D3M meta-learning database. All that's required is credentials for accessing it. This package uses the `elasticsearch-dsl` python package to query.

## Getting Started

### Installation:

1.  Clone this repository

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

#### Make a Full Dump

To make a full JSON dump of the D3M MtL database, run this. Note: It copies all the indexes in the D3M elasticsearch instance and will take some time.

```shell
python src/dump.py [--out-dir dump_dir_name] [--batch-size num_docs_in_batch]
```

The dump will default to being written to the `dump` directory within the current working directory.

#### Query the Database

Using the `elasticsearch-dsl` python package, the database can be queried programmatically, with a fair amount of ease and flexibility.

1.  Import `client` from the local `client` module.
1.  Import `INDEXES` from the local `settings` module.
1.  Use the [`elasticsearch-dsl` documentation](https://elasticsearch-dsl.readthedocs.io/en/latest/search_dsl.html) to begin querying the elasticsearch indexes. Example:
    
    ```python
    from client import client
    from settings import INDEXES

    # Search all pipeline documents (defaults to only returning 10 at a time max)
    s = Search(using=client, index=INDEXES["PIPELINES"])

    # Execute the search
    response = s.execute()

    # Print each pipeline returned by the search.
    for hit in response:
        print(hit.id)
    ```

    The `elasticsearch-dsl` package has support for filtering, aggregating, sorting, and querying.