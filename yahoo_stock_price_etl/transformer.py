##################################################################
# stock_price_data_etl
# This module contains the StockDataTransformer class, which is 
# responsible for transforming stock data.
# It includes methods for cleaning data, adding rolling averages, 
# and calculating daily returns.        
##################################################################

import pandas as pd
from typing import Any, Dict, List
from yahoo_stock_price_etl import BaseETL

ROLL_AVG_DAYS_COLUMN_TEMPLATE = "RollAvg{window}Days"
EMA_COLUMN_TEMPLATE = "EMA{span}"
TARGET_FILE_NAME = "transformed_data"


class StockDataTransformer(BaseETL):
    """
    Cleans and transforms stock data: handles missing values, calculates rolling averages, 
    daily returns, etc.

    :param tickers: List of stock ticker symbols.
    :param extraction_date_str: Date string for the extraction date (YYYY_MM_DD).
    :param config: Configuration dictionary for the ETL process.
    """
    def __init__(
        self, 
        tickers: Any, 
        extraction_date_str: str, 
        config: Dict[str, Any],
        common_config: Dict[str, Any],
    ) -> None:
        super().__init__(common_config=common_config)

        self.config: Dict[str, Any] = config
        self.tickers: List[str] = tickers if isinstance(tickers, list) else [tickers,]
        self.extraction_date_str: str = extraction_date_str
        
        self.df = self._read_and_concatenate()

    def _read_and_concatenate(self) -> pd.DataFrame:
        """
        Reads multiple DataFrames and concatenates them into a single MultiIndex DataFrame 
        (Date, Ticker).

        :return: Multi-indexed DataFrame.
        """
        base_path = f"{self._get_path('extract')}/{self.extraction_date_str}"
        
        dfs: List[pd.DataFrame] = []
        for ticker in self.tickers:
            df = pd.read_csv(f"{base_path}/{ticker}.csv", index_col=None, header=0)
            # Ensure 'Date' is a column and is datetime
            df['Date'] = pd.to_datetime(df['Date'])
            df['Ticker'] = ticker
            dfs.append(df)

        # Concat DataFrames and Set MultiIndex
        df_all = pd.concat(dfs, ignore_index=True)
        df_all.set_index(['Date', 'Ticker'], inplace=True)
        return df_all

    def _clean(self) -> None:
        """
        Cleans the DataFrame by filling missing values.
        Fills forward for stock prices and backfills for volume.
        :return: None. Modifies the DataFrame in-place.
        """
        for col in self.df.columns:

            # Fill forward for stock prices
            if col in ["Open", "High", "Low", "Close", "Adj Close"]:
                self.df[col] = self.df[col].ffill()

            # Backfill for volume
            elif col == "Volume":
                self.df[col] = self.df[col].bfill()

    def _add_daily_return(self) -> None:
        """
        Calculate the daily return for each stock ((Close Price Today -
        Close Price Yesterday) / Close Price Yesterday).
        
        :return: None. Adds the daily return column in-place."""
        self.df["DailyReturn"] = self.df.groupby("Ticker")["Close"].pct_change()

    def _add_rolling_average(self, window: int = 30) -> None:
        """
        Compute a "window"-day rolling average of the closing price for each 
        stock (grouped by Ticker).

        :param window: The number of days for the rolling average.
        :return: None. Adds the rolling average column in-place.
        """
        self.df[ROLL_AVG_DAYS_COLUMN_TEMPLATE.format(window=window)] = (
            self.df.groupby(level="Ticker")["Close"]
            .rolling(window=window, min_periods=1)
            .mean()
            .reset_index(level=0, drop=True)
        )

    def _add_ema(self, span: int = 14) -> None:
        """
        Exponential Moving Average (EMA): Calculate the "span"-day EMA for each 
        stock.

        :param span: The span for the EMA calculation.
        :return: None. Adds the EMA column in-place.
        """
        self.df[EMA_COLUMN_TEMPLATE.format(span=span)] = (
            self.df.groupby(level="Ticker")["Close"]
            .ewm(span=span, adjust=False)
            .mean()
            .reset_index(level=0, drop=True)
        )

    def _add_time_series_analysis(self, roll_window: int = 30, ema_span: int = 14) -> None:
        """
        Create a 'Crossover' column: 1 if 30-day avg > 14-day EMA, -1 if <, 0 if equal, per stock and day.
        """
        roll_col = ROLL_AVG_DAYS_COLUMN_TEMPLATE.format(window=roll_window)
        ema_col = EMA_COLUMN_TEMPLATE.format(span=ema_span)
        # Compare rolling average and EMA
        crossover = (self.df[roll_col] > self.df[ema_col]).astype(int) - (self.df[roll_col] < self.df[ema_col]).astype(int)
        self.df["Crossover"] = crossover

    def transform(self) -> pd.DataFrame:
        """
        Transforms the stock data by cleaning, adding daily returns, rolling averages,
        exponential moving averages, and time series analysis.
        
        :return: Transformed DataFrame.
        """
        self._clean()

        self._add_daily_return()
        self._add_rolling_average(window=self.config["roll_avg_days"])
        self._add_ema(span=self.config["ema_span"])
        self._add_time_series_analysis(
            roll_window=self.config["roll_avg_days"], 
            ema_span=self.config["ema_span"],
        )

        self._save_to_csv(
            base_path=self._get_path("transform"),
            path=[self.extraction_date_str,],
            df=self.df,
            file_name=TARGET_FILE_NAME,
        )

        return self.df
