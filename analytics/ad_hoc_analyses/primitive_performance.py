import csv

from fire import Fire

from analytics.databases.aml_client import AMLDB


def main(path: str = "primitive-performances.csv"):
    """
    Finds the average score each primitive gets on each problem
    type, as well as a couple other metrics, writing the results
    to a csv file. Useful for getting an idea of what the best
    primitives are for each problem type.
    
    Parameters
    ----------
    path : str
        The path the final csv file will be written to.
    """
    aml = AMLDB()

    results = aml.db.pipeline_runs.aggregate(
        [
            # Only get successful pipelines that have scored results.
            {"$match": {"status.state": "SUCCESS", "run.phase": "PRODUCE"}},
            # Expand on each task keyword in the run's problem.
            {
                "$unwind": {
                    "path": "$problem.problem.task_keywords",
                    "preserveNullAndEmptyArrays": False,
                }
            },
            # Expand on each metric the run was scored on.
            {
                "$unwind": {
                    "path": "$run.results.scores",
                    "preserveNullAndEmptyArrays": False,
                }
            },
            # Expand on each primitive in the run's pipeline.
            {
                "$unwind": {
                    "path": "$pipeline.steps",
                    "preserveNullAndEmptyArrays": False,
                }
            },
            # Group by metric, primitive, and task keyword, finding the average and
            # standard deviation of the scores for each metric + primitive + task keyword
            # combination.
            {
                "$group": {
                    "_id": {
                        "metric": "$run.results.scores.metric.metric",
                        "primitive": "$pipeline.steps.primitive.python_path",
                        "task_keyword": "$problem.problem.task_keywords",
                    },
                    "mean_score": {"$avg": "$run.results.scores.value"},
                    "mean_normalized_score": {"$avg": "$run.results.scores.normalized"},
                    "std_normalized_score": {
                        "$stdDevSamp": "$run.results.scores.normalized"
                    },
                    "num_occurences": {"$sum": 1},
                }
            },
        ]
    )

    results = list(results)

    # Unnest the _id field components.
    _id_fields = ["metric", "primitive", "task_keyword"]
    for result in results:
        for field in _id_fields:
            result[field] = result["_id"][field]
        del result["_id"]

    # Write the results to a CSV file
    with open("primitive-performances.csv", "w") as f:
        fieldnames = [
            "primitive",
            "task_keyword",
            "num_occurences",
            "metric",
            "mean_score",
            "mean_normalized_score",
            "std_normalized_score",
        ]
        writer = csv.DictWriter(f, fieldnames)
        writer.writeheader()
        writer.writerows(results)


if __name__ == "__main__":
    Fire(main)
