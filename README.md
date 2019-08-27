[![Build Status](https://api.travis-ci.org/byu-dml/d3m-mtl-db-reader.png)](https://travis-ci.org/byu-dml/d3m-mtl-db-reader)
[![codecov](https://codecov.io/gh/byu-dml/d3m-mtl-db-reader/branch/master/graph/badge.svg)](https://codecov.io/gh/byu-dml/d3m-mtl-db-reader)

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
    DATA_ROOT=<base_dir_for_package_files>
    ```

    `DATA_ROOT` is the base directory where all the package's DB dumps, DB extracts, caches, and other files are/will be stored. Defaults to the current directory.

## Usage

### Query the Database

Using the `elasticsearch-dsl` python package, the database can be queried programmatically, with a fair amount of ease and flexibility.

1.  Import the `client` object from the local `client` module.
1.  Import the `Index` enum from the local `settings` module.
1.  Import the `Search` class from the `elasticsearch_dsl` package.
1.  Use the [`elasticsearch-dsl` documentation](https://elasticsearch-dsl.readthedocs.io/en/latest/search_dsl.html) to begin querying the elasticsearch indexes. Example:
    
    ```python
    from elasticsearch_dsl import Search
    
    from src.client import client
    from src.misc.settings import Index

    # Search all pipeline documents (defaults to only returning 10 at a time max)
    s = Search(using=client, index=Index.PIPELINES.value)

    # Execute the search
    response = s.execute()

    # Print each pipeline returned by the search.
    for hit in response:
        print(hit.id)
    ```

    The `elasticsearch-dsl` package has support for filtering, aggregating, sorting, and querying, but not joining. A pickled, denormalized extraction of the database's documents can be created (see below for instructions), and is useful for analysis that involves joining.

### Get Basic DB Stats

To query the number of documents contained in each index of the DB, run:

```shell
python -m src.count
```

This is useful for finding out how big the database is.

### Make a Full Dump

To make a full JSON dump of the D3M MtL database, run this. Note: It copies all the indexes in the D3M elasticsearch instance and will take some time, depending on the number of documents that exist.

```shell
python -m src.dump [--batch-size <num_docs_in_batch>] [--indexes <index_names_to_dump>] [--predictions]
```

`--batch-size` is an optional optimization helper that allows one to specify how many documents should be requested in each network request made to the D3M elasticsearch instance.

`--indexes` can be used to specify the list of index names wanting to be dumped. If left out, all indexes will be dumped.

If the `--predictions` flag is present, prediction scores for all pipeline runs will be dumped. Note: This is a considerable amount of data. 

### Extract a dump

To extract a denormalized map of all the DB documents into a form ready for analysis, run:

```shell
python -m src.extract [--index-name <pipeline_runs_index_name>]
```

The extraction made is a dictionary of index names to indexes. Each index is a dictionary of denormalized documents to their ids.

`--index-name` is an optional argument specifying the name of the pipeline runs index to use, since there are two of them. The default choice should be sufficient for most uses.

### Analyze an Extraction

To analyze a denormalized extraction of pipeline runs, run this:

```shell
python -m src.analyze [--analysis <name_of_analysis_to_run>] [--verbose] [--refresh]
```

If the `--verbose` flag is present, the results of the analysis will be reported more verbosely, if the analysis has verbose per-pipeline results to report that is.

If the `--refresh` flag is present, any cached function calls an analysis computes and uses will be refreshed.

To see which analyses are currently supported, run `python -m src.analyze --help` and look at the available options for the `--analysis` arg.
