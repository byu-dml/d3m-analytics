[![Build Status](https://api.travis-ci.org/byu-dml/d3m-mtl-db-reader.png)](https://travis-ci.org/byu-dml/d3m-mtl-db-reader)

# Programmatically Interact With The D3M MtL Database 

You can use this package to programmatically interact with the D3M meta-learning database. All that's required is credentials for accessing it. This package uses the `elasticsearch-dsl` python package to query.

## Installation:

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

## Usage

### Query the Database

Using the `elasticsearch-dsl` python package, the database can be queried programmatically, with a fair amount of ease and flexibility.

1.  Import the `client` object from the local `client` module.
1.  Import the `Index` enum from the local `settings` module.
1.  Import the `Search` class from the `elasticsearch_dsl` package.
1.  Use the [`elasticsearch-dsl` documentation](https://elasticsearch-dsl.readthedocs.io/en/latest/search_dsl.html) to begin querying the elasticsearch indexes. Example:
    
    ```python
    from src.client import client
    from src.settings import Index
    from elasticsearch_dsl import Search

    # Search all pipeline documents (defaults to only returning 10 at a time max)
    s = Search(using=client, index=Index.PIPELINES.value)

    # Execute the search
    response = s.execute()

    # Print each pipeline returned by the search.
    for hit in response:
        print(hit.id)
    ```

    The `elasticsearch-dsl` package has support for filtering, aggregating, sorting, and querying, but not joining. A pickled, denormalized extraction of pipeline runs can be created (see below for instructions), and is useful for analysis that involves joining.

### Make a Full Dump

To make a full JSON dump of the D3M MtL database, run this. Note: It copies all the indexes in the D3M elasticsearch instance and will take some time, depending on the number of documents that exist.

```shell
python -m src.dump [--out-dir dump_dir_name] [--batch-size num_docs_in_batch] [--count]
```

The dump will default to being written to the `dump` directory within the current working directory.

If the `--count` arg is present, the database will not be dumped. Rather, the system will simply query the number of documents contained in each index. This is useful for finding out how big the database is.

### Extract a dump

To extract a denormalized map of pipeline runs from a dump into a form ready for analysis, run this:

```shell
python -m src.extractor [--dump-dir dir_containing_dump] [--out-dir dir_to_pickle_to] [--index-name pipeline_runs_index_name] [--dont-enforce-ids]
```

The extraction will be pickled to `--out-dir`, and is a dictionary of pipeline run digests to pipeline runs.

If `--dont-enforce-ids` is included, the system will not throw an error if a document does not have an ID/digest (not recommended).

### Analyze an Extraction

To analyze a denormalized extraction of pipeline runs, run this:

```shell
python -m src.analyze [--pkl-dir dir_containing_pickle] [--analysis name_of_analysis_to_run] [--verbose]
```

If `--verbose` is present, the results of the analysis will be reported more verbosely, if the analysis has verbose per-pipeline results to report that is.

To see which analyses are currently supported, run `python -m src.analyze --help` and look at the available options for the `--analysis` arg.