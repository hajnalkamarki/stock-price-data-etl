##################################################################
##################################################################
# stock_price_data_etl
# This module contains the StockDataExtractor class to extract 
# stock data from Yahoo Finance using yfinance.
##################################################################
##################################################################
import yfinance as yf
import pandas as pd
from typing import Any, Dict, List

from datetime import datetime
from yahoo_stock_price_etl import BaseETL


class StockDataExtractor(BaseETL):
    """
    Extracts historical stock data from Yahoo Finance using yfinance.
    Saves each stock's raw data to a CSV file under /extract/<date>/<stock>.csv
    """
    def __init__(
        self, 
        tickers: Any, 
        start: str, 
        end: str, 
        common_config: Dict[str, Any],
    ) -> None:
        """
        Initializes the StockDataExtractor.

        :param tickers: Single ticker symbol or list of ticker symbols.
        :param start: Start date for the data extraction (YYYY-MM-DD).
        :param end: End date for the data extraction (YYYY-MM-DD).
        :param config: Configuration dictionary for the ETL process.
        """
        super().__init__(common_config=common_config)

        self.tickers: List[str] = tickers if isinstance(tickers, list) else [tickers,]
        self.start: str = start
        self.end: str = end

    def _set_date(self) -> None:
        """
        Sets the date range for the extraction.
        """
        self.day_str = datetime.today().strftime("%Y_%m_%d")

    def extract(self) -> pd.DataFrame:
        """
        Extracts stock data for the specified tickers and date range.

        :return: DataFrame containing the stock data.
        """
        self._set_date()
        base_path = self._get_path("extract")
        
        for ticker in self.tickers:
            df = yf.download(ticker, start=self.start, end=self.end)
            # If columns are MultiIndex, flatten to keep only the first level
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = df.columns.get_level_values(0)
            df = df.reset_index()  # Ensure 'Date' is a column
            # print(df.head())

            self._save_to_csv(
                base_path=base_path,
                path=[self.day_str,],
                df=df,
                file_name=ticker,
            )

        return df
