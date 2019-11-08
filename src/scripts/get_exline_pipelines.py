"""
Queries two git repositories to extract D3M Exline pipeline
definitions and meta data identifying the problems they're intended
to tackle.
"""

from collections import defaultdict
import subprocess
from typing import Dict, List, Set, Optional, Tuple
import os
import glob
import re
import shutil
import itertools
import filecmp

# Helper functions
####################


def copy_files_by_glob(
    pattern: str, save_dir: str, collector: Optional[Dict[str, Set[str]]]
):
    """
    Parameters
    ----------
    pattern
        The glob pattern to match to
    save_dir
        The directory path to save all files matching `pattern` to.
    collector
        If supplied, will collect all file paths matching `pattern`
        into a set mapped to the file paths' file name.
    """
    if not os.path.isdir(save_dir):
        os.makedirs(save_dir)

    for path in glob.glob(pattern):
        file_name = path.split("/")[-1]
        if collector is not None:
            collector[file_name].add(path)
        save_path = f"{save_dir}/{file_name}"
        shutil.copy(path, save_path)


def cmpfilelist(filepaths: List[str], shallow: bool = True):
    """
    Compares the contents of all possible pairs of files
    in `filepaths`. Similar to `filecmp.cmpfiles` but takes
    a list of filepaths rather than two directories, so the
    files in `filepaths` can be in any directories, not limited
    to just two.

    Returns three lists of file names: `match`, `mismatch`,
    `errors`. `match` contains the list of file pair tuples that
    match, `mismatch` contains the names of those that don’t,
    and `errors` lists the names of files which could not be
    compared.
    """
    match: List[Tuple[str, str]] = []
    mismatch: List[Tuple[str, str]] = []
    errors: List[Tuple[str, str]] = []
    for path_a, path_b in itertools.combinations(filepaths, 2):
        try:
            if filecmp.cmp(path_a, path_b, shallow):
                match.append((path_a, path_b))
            else:
                mismatch.append((path_a, path_b))
        except Exception:
            errors.append((path_a, path_b))
    return match, mismatch, errors


# Define script data
####################

data_dir = "script-data/exline_repos"  # dir to clone the git repos to.
save_dir = "dump/exlines"  # dir to save the final exline pipeline and meta files to.
current_d3m_version = "v2019.6.7"
python_path_field_pattern = re.compile(r"['|\"]python_path['|\"]:\s.?['|\"](.+)['|\"],")
python_paths_to_exclude_from_exlines: Set[str] = {
    # "OutputDataframe" is just a debug primitive. It is not an exline.
    "d3m.primitives.data_transformation.data_cleaning.OutputDataframe"
}

# Define git repositories data
####################

# repo with master list of Exline primitives
exline_primitive_list_repo: Dict[str, str] = {
    "url": "https://github.com/uncharted-distil/distil-primitives.git",
    "save_dir": f"{data_dir}/primitives_list",
    "target_dir": f"{data_dir}/primitives_list/distil/primitives",
}

# repo with Exline pipeline definitions
pipelines_repo: Dict[str, str] = {
    "url": "https://gitlab.com/datadrivendiscovery/primitives.git",
    "save_dir": f"{data_dir}/pipelines",
    "target_dir": f"{data_dir}/pipelines/{current_d3m_version}/Distil",
}

# Clone the git repos
####################

for repo_meta in (exline_primitive_list_repo, pipelines_repo):
    if not os.path.isfile(f"{repo_meta['save_dir']}/README.md"):
        # This repo has not been cloned yet.
        print(f"Cloning repo '{repo_meta['url']}'")
        subprocess.call(["git", "clone", repo_meta["url"], repo_meta["save_dir"]])

# Get the python paths of the Exline primitives from `exline_primitive_list_repo`
####################

primitive_python_paths: Set[str] = set()
for primitive_file_path in glob.glob(
    f"{exline_primitive_list_repo['target_dir']}/*.py"
):
    with open(primitive_file_path, "r") as prim_file:
        for line in prim_file.readlines():
            matches = python_path_field_pattern.search(line)
            if matches:
                primitive_python_paths.add(matches[1])

primitive_python_paths = primitive_python_paths.difference(
    python_paths_to_exclude_from_exlines
)

print(f"Found {len(primitive_python_paths)} Exline primitive_python_paths.")

# Now get the pipelines for those repos, along with the problem for each pipeline,
# saving them to `save_dir`.
####################

pipeline_paths_by_pipeline_name: defaultdict = defaultdict(set)
meta_paths_by_meta_name: defaultdict = defaultdict(set)

for python_path in primitive_python_paths:
    primitive_dir = f"{pipelines_repo['target_dir']}/{python_path}"
    assert os.path.isdir(primitive_dir), f"'{primitive_dir}' is not a directory"
    pipelines_glob = f"{primitive_dir}/*/pipelines/*.json"
    pipelines_meta_glob = f"{primitive_dir}/*/pipelines/*.meta"

    if not os.path.isdir(save_dir):
        os.makedirs(save_dir)

    copy_files_by_glob(pipelines_glob, save_dir, pipeline_paths_by_pipeline_name)
    copy_files_by_glob(pipelines_meta_glob, save_dir, meta_paths_by_meta_name)

print(f"Saved all exline pipeline and meta files to '{save_dir}'.")

# As a check, compare all the pipeline and meta files that have the same name,
# to see which ones are identical or not.
####################

for paths in pipeline_paths_by_pipeline_name.values():
    if len(paths) > 1:
        match, mismatch, errors = cmpfilelist(paths, shallow=False)
        assert len(mismatch) == 0 and len(errors) == 0, (
            "Some pipeline file contents did not match or could not be compared, "
            "even though they had the same pipeline id"
        )

for paths in meta_paths_by_meta_name.values():
    if len(paths) > 1:
        match, mismatch, errors = cmpfilelist(paths, shallow=False)
        assert len(mismatch) == 0 and len(errors) == 0, (
            "Some meta file contents did not match or could not be compared, "
            "even though they had the same pipeline id"
        )

print(
    "Integrity checks passed. All pipeline and meta files with duplicate pipeline ID "
    "file names have identical contents."
)
print("Finished.")
