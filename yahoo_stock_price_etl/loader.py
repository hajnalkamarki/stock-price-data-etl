import pandas as pd
from typing import Any, Dict

SOURCE_FILE_NAME = "transformed_data"


from yahoo_stock_price_etl import BaseETL

class StockDataLoader(BaseETL):
    """
    Loads and saves transformed stock data (the 'load' step in ETL).
    """
    def __init__(
        self, 
        extraction_date_str: str, 
        config: Dict[str, Any],
        common_config: Dict[str, Any],
    ) -> None:
        super().__init__(common_config=common_config)

        self.config: Dict[str, Any] = config
        self.extraction_date_str: str = extraction_date_str
        self.df = self._read()
        
        print(self.df.head())

    def _read(self) -> pd.DataFrame:
        """
        Reads multiple DataFrames and concatenates them into a single MultiIndex DataFrame 
        (Date, Ticker).

        :return: Multi-indexed DataFrame.
        """
        base_path = f"{self._get_path('transform')}/{self.extraction_date_str}"

        return pd.read_csv(f"{base_path}/{SOURCE_FILE_NAME}.csv", index_col=None, header=0)

    def load(self) -> pd.DataFrame:
        """
        Loads the DataFrame and returns it.
        
        :return: DataFrame containing the loaded data.
        """
        return self.df
