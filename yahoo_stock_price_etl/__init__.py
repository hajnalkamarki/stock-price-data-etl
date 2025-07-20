import os
import pandas as pd

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Literal


class BaseETL(ABC):
    def __init__(self, common_config: Dict[str, Any]) -> None:
        """
        Initialize the ETL process with the given configuration.
        
        :param config: Configuration dictionary for the ETL process.
        """
        self.common_config = common_config

    def _get_path(self, phase_name: Literal["extract", "transform", "load"]) -> str:
        """
        Get the base path for saving files.
        
        :return: Base path as a string.
        """
        return "{common_path}{phase_name}".format( 
            common_path=self.common_config.get('file_path'),
            phase_name=phase_name,
        )
    
    def _save_to_csv(self, base_path: str, path: List[str], df: pd.DataFrame, file_name: str):
        """
        Saves the DataFrame to a CSV file.

        :param df: DataFrame to save.
        :param file_name: File name.
        """
        dir_path = os.path.join(base_path, *path)
        os.makedirs(dir_path, exist_ok=True)
        out_file = os.path.join(dir_path, f"{file_name}.csv")
        df.to_csv(out_file, index=False)
