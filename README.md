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
    MONGO_HOST=<host_the_lab_db_is_on>
    MONGO_PORT=<port_the_lab_db_is_on>
    ```

    `DATA_ROOT` is the base directory where all the package's DB caches, and other files are/will be stored. Defaults to the current directory.

Note: When contributing to this repo, there are additional steps to take. See the "Contributing" section below.

## Usage

### Get Basic DB Stats

To query the number of documents contained in each index of the D3M DB, run:

```shell
python -m analytics.count
```

This is useful for finding out how big the database is.

### Query The Lab's Database

To perform queries on the lab's local mongodb database, connect to the database using your `MONGO_HOST` and `MONGO_PORT` environment variables and use any query tool that mongodb supports. For convenience, a `pymongo` client already connected to the database is exposed in `analytics.databases.aml_client`, e.g.:

```python
from analytics.databases.aml_client import AMLDB

# Get the number of pipelines in the lab's database:
aml = AMLDB()
pipeline_count = aml.db.pipelines.estimated_document_count()
```

Here `aml.db.pipelines` is an instance of `pymongo.collection.Collection`. 

### Sync D3M's Data to The Lab's Database

To copy D3M's data down into our local database, run: 

```
python -m analytics.sync [--batch-size num_docs_in_batch] [--indexes index_names_to_sync]
```

`--batch-size` is an optional optimization helper that allows one to specify how many documents should be requested in each network request made to the D3M elasticsearch instance.

`--indexes` can be used to specify the list of index names wanting to be synced. If left out, all indexes will be synced.

### Computing And Adding Metafeatures to the Datasets Collection

To compute metafeatures on all dataset living on the AML lab's file server, and add them to the dataset references living in the AML DB, run:

```
python -m analytics.metafeatures [problems_dirs] --max_rows <max_dataset_rows>
```

`problem_dirs` is an optional list of directory paths which house dataset/problem directories as their immediate subchildren. If not provided, metafeatures will be computed for all datasets/problems found under the directories listed in `analytics.misc.settings.dataset_directories`. `--max_rows` filters datasets. Datasets having more than `max_rows` will be skipped.

### Denormalizing the Pipeline Runs Collection

To denormalize the `pipeline_runs` collection and replace each pipeline run's references to any datasets, problems, and pipelines with actual copies of the dataset, problem, and pipeline documents, run:

```
python -m analytics.denormalize [--batch_size you_batch_size]
```

`--batch_size` is an optional argument which determines the number of documents to read and write in each batch call of queries made to the database.

## Contributing

### pre-commit

This repo uses the [pre-commit](https://pre-commit.com/#intro) package to include pre-commit git hooks for things like auto formatting and linting. Once this repo is cloned, run `pre-commit install` in the repo's root directory to initialize the pre-commit package for the repo.

Every time a commit takes place, pre-commit will automatically run, and abort the commit if it finds errors. If you want are doing a partial commit on one or more files (committing part of the changes to a file but not all of them), you'll need to skip the pre-commit hook by using the `--no-verify` option when commiting e.g. `git commit --no-verify`. Then in the commit that includes all of the remaining changes to that file, be sure to run the pre-commit hook, which will take place using the normal `git commit` command.
