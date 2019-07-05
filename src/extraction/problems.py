from src.entities.problem import Problem
from src.extraction.loader import load_index
from src.settings import Indexes


def load_problems(dump_path: str, should_enforce_id: bool) -> dict:
    """
    Loads a map of problems from the dump_path.
    Returns a dictionary map of each problem digest to its problem.
    """
    problems = {}

    for problem_dict in load_index(dump_path, Indexes.PROBLEMS.value):
        problem = Problem(problem_dict, should_enforce_id)
        problems[problem.digest] = problem

    return problems
