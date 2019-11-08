[![Build Status](https://api.travis-ci.org/byu-dml/d3m-mtl-db-reader.png)](https://travis-ci.org/byu-dml/d3m-mtl-db-reader)
[![codecov](https://codecov.io/gh/byu-dml/d3m-mtl-db-reader/branch/master/graph/badge.svg)](https://codecov.io/gh/byu-dml/d3m-mtl-db-reader)

# Programmatically Interact With The D3M MtL Database

You can use this package to programmatically interact with the D3M meta-learning database. This package uses the `elasticsearch-dsl` python package to query.

## Installation:

1.  Clone this repository

1.  ```shell
    pip install -r requirements.txt
    ```

1.  Add an `.env` file to root, and include these values:

    ```env
    DATA_ROOT=<base_dir_for_package_files>
    ```

    `DATA_ROOT` is the base directory where all the package's DB dumps, DB extracts, caches, and other files are/will be stored. Defaults to the current directory.

Note: When contributing to this repo, there are additional steps to take. See the "Contributing" section below.

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
python -m src.dump [--batch-size num_docs_in_batch] [--indexes index_names_to_dump] [--predictions]
```

`--batch-size` is an optional optimization helper that allows one to specify how many documents should be requested in each network request made to the D3M elasticsearch instance.

`--indexes` can be used to specify the list of index names wanting to be dumped. If left out, all indexes will be dumped.

If the `--predictions` flag is present, prediction scores for all pipeline runs will be dumped. Note: This is a considerable amount of data.

### Inspect a Dump

Once a dump of the database is available, the `inspect` CLI is a useful tool for piping the contents of a DB index to stdout. Usage:

```shell
python -m src.inspect [--index index_to_pipe] [--predictions-id pipeline_run_id]
```

If `--predictions-id` is supplied, the predictions document identified by `pipeline_run_id` will be piped to stdout. Otherwise, the index identified by `index_to_pipe` will be piped. The default for `index_to_pipe` is the pipeline run documents.

For example, to search through the pipeline documents for all pipelines that contain the string `random_forest`, run:

```shell
python -m src.inpsect --index pipelines | grep random_forest
```

### Extract a dump

To extract a denormalized map of all the DB documents into a form ready for analysis, run:

```shell
python -m src.extract [--index-name pipeline_runs_index_name]
```

The extraction made is a dictionary of index names to indexes. Each index is a dictionary of denormalized documents to their ids.

`--index-name` is an optional argument specifying the name of the pipeline runs index to use, since there are two of them. The default choice should be sufficient for most uses.

### Make Aggregations from an Extraction

You can run aggregations on an extraction with this command:

```shell
python -m src.aggregate [name_of_aggregation_to_run] [--verbose | -v] [--refresh | -r]
```

There are a number of aggregations available to be run on the extracted data. The implemented aggregations can be seen listed under `positional arguments` after running the command `python -m src.aggregate --help`.

When the above command is run, the results of the aggregation are saved in a CSV table(s) for exporting. There are also some analyses that depend on the results of certain aggregations. When these analyses are run, the aggregation(s) will be run fresh—regardless of whether it has been completed before—and new tables _will not_ be saved.

If the `--verbose` flag is present, the results of the aggregation will be reported more verbosely, if the aggregation has verbose per-pipeline results to report.

If the `--refresh` flag is present, any cached function calls an aggregation computes and uses will be refreshed.

### Analyze an Extraction

To analyze a denormalized extraction of pipeline runs, run this:

```shell
python -m src.analyze [--analysis name_of_analysis_to_run] [--verbose] [--refresh]
```

If the `--verbose` flag is present, the results of the analysis will be reported more verbosely, if the analysis has verbose per-pipeline results to report that is.

If the `--refresh` flag is present, any cached function calls an analysis computes and uses will be refreshed.

To see which analyses are currently supported, run `python -m src.analyze --help` and look at the available options for the `--analysis` arg.

## Contributing

### pre-commit

This repo uses the [pre-commit](https://pre-commit.com/#intro) package to include pre-commit git hooks for things like auto formatting and linting. Once this repo is cloned, run `pre-commit install` in the repo's root directory to initialize the pre-commit package for the repo.

Every time a commit takes place, pre-commit will automatically run, and abort the commit if it finds errors. If you want are doing a partial commit on one or more files (committing part of the changes to a file but not all of them), you'll need to skip the pre-commit hook by using the `--no-verify` option when commiting e.g. `git commit --no-verify`. Then in the commit that includes all of the remaining changes to that file, be sure to run the pre-commit hook, which will take place using the normal `git commit` command.
