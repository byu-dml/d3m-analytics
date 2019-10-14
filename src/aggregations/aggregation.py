import csv, uuid, os
from abc import ABC, abstractmethod
from typing import Dict, List, Union

from src.misc.settings import DataDir

class Aggregation(ABC):
    """
    An abstract class inherited by all aggregation statistics
    that are run by the `src.aggregate` module.
    """

    @abstractmethod
    def run(self, entity_maps: Dict[str, dict], verbose: bool, refresh: bool, save_table: bool):
        """
        Parameters
        ----------
        entity_maps
            A dictionary mapping extraction names to extraction dictionaries.
            Contains extraction dictionaries for pipeline runs, problems,
            pipelines, and datasets.
        verbose
            If true, the aggregation will output detailed information on its
            progress and intermediate results during computation, if applicable
        refresh
            If true, disable caching while running the aggregation
        """
        pass

    def save_table(self, table_name: str, fields: List[str],
        data: List[Dict[str, Union[str, int, float]]], key: str=None):
        """
        Saves calculated aggregate data as a table where it can be exported to
        e.g. Tableau for manual exploration.

        Parameters
        ----------
        table_name
            The string to use for the table's name.
        fields
            Complete list of columns in the table.
        data
            List of dictionaries representing the table. Each row is a dictionary
            whose keys are the column names of the table. It is not required that
            all the field names found in `fields` are present in every dictionary.
        key
            A string present in the list `fields` to use as the table's primary
            key. If the value of `key` is not found in `fields`, a UUID will be
            generated for each row in the table and named with the value of `key`.
            Default of None means to use the first value in `fields` as the key.
        """
        if key is None:
            key = fields[0]

        if key not in fields:
            for record in data:
                record[key] = str(uuid.uuid4())

        csv_dir = os.path.join(DataDir.AGGREGATION.value, self.__class__.__name__, 'csv')
        if not os.path.isdir(csv_dir):
            os.makedirs(csv_dir)

        csv_path = os.path.join(csv_dir, table_name+'.csv')
        with open(csv_path, 'w+', newline='') as csv_file:
            csv_writer = csv.DictWriter(csv_file, fields)
            csv_writer.writeheader()
            csv_writer.writerows(data)
