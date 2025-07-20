import unittest
import pandas as pd
from unittest.mock import patch
from yahoo_stock_price_etl.transformer import StockDataTransformer


class TestStockDataTransformer(unittest.TestCase):
    def setUp(self):
        self.tickers = ["AAPL", "MSFT"]
        self.extraction_date_str = "2023_01_01"
        self.config = {"roll_avg_days": 2, "ema_span": 2}
        self.common_config = {"file_path": "/tmp"}
        # Create mock DataFrames for each ticker
        self.mock_data = {}
        date_range = pd.date_range("2023-01-01", periods=5)
        for ticker in self.tickers:
            df = pd.DataFrame({
                "Date": date_range,
                "Open": [1, 2, 3, 4, 5],
                "High": [2, 3, 4, 5, 6],
                "Low": [0, 1, 2, 3, 4],
                "Close": [1, 2, 3, 4, 5],
                "Adj Close": [1, 2, 3, 4, 5],
                "Volume": [100, 200, 300, 400, 500],
            })
            self.mock_data[ticker] = df

    def mock_read_and_concatenate(self):
        dfs = []
        for ticker in self.tickers:
            df = self.mock_data[ticker].copy()
            df['Date'] = pd.to_datetime(df['Date'])
            df['Ticker'] = ticker
            dfs.append(df)
        df_all = pd.concat(dfs, ignore_index=True)
        df_all.set_index(['Date', 'Ticker'], inplace=True)
        return df_all

    @patch.object(StockDataTransformer, '_read_and_concatenate')
    @patch.object(StockDataTransformer, '_save_to_csv')
    def test_transform_returns_dataframe(self, mock_save, mock_read):
        mock_read.side_effect = self.mock_read_and_concatenate
        transformer = StockDataTransformer(self.tickers, self.extraction_date_str, self.config, self.common_config)
        df = transformer.transform()
        self.assertIsInstance(df, pd.DataFrame)
        self.assertIn("DailyReturn", df.columns)
        self.assertIn("Crossover", df.columns)

    @patch.object(StockDataTransformer, '_read_and_concatenate')
    @patch.object(StockDataTransformer, '_save_to_csv')
    def test_transform_rolling_average_and_ema(self, mock_save, mock_read):
        mock_read.side_effect = self.mock_read_and_concatenate
        transformer = StockDataTransformer(self.tickers, self.extraction_date_str, self.config, self.common_config)
        df = transformer.transform()
        roll_col = f"RollAvg{self.config['roll_avg_days']}Days"
        ema_col = f"EMA{self.config['ema_span']}"
        self.assertIn(roll_col, df.columns)
        self.assertIn(ema_col, df.columns)
        self.assertTrue(df[roll_col].notna().any())
        self.assertTrue(df[ema_col].notna().any())

    @patch.object(StockDataTransformer, '_read_and_concatenate')
    @patch.object(StockDataTransformer, '_save_to_csv')
    def test_transform_cleaning(self, mock_save, mock_read):
        # Introduce NaNs in the input and check that they are filled
        def mock_read_and_concatenate_with_nan():
            dfs = []
            for ticker in self.tickers:
                df = self.mock_data[ticker].copy()
                df['Date'] = pd.to_datetime(df['Date'])
                df['Ticker'] = ticker
                if ticker == self.tickers[0]:
                    df.loc[2, "Close"] = None
                dfs.append(df)
            df_all = pd.concat(dfs, ignore_index=True)
            df_all.set_index(['Date', 'Ticker'], inplace=True)
            return df_all
        mock_read.side_effect = mock_read_and_concatenate_with_nan
        transformer = StockDataTransformer(self.tickers, self.extraction_date_str, self.config, self.common_config)
        df = transformer.transform()
        self.assertFalse(df["Close"].isna().any())

    @patch.object(StockDataTransformer, '_read_and_concatenate')
    @patch.object(StockDataTransformer, '_save_to_csv')
    def test_transform_multiindex(self, mock_save, mock_read):
        mock_read.side_effect = self.mock_read_and_concatenate
        transformer = StockDataTransformer(self.tickers, self.extraction_date_str, self.config, self.common_config)
        df = transformer.transform()
        self.assertTrue(isinstance(df.index, pd.MultiIndex))
        self.assertEqual(list(df.index.names), ["Date", "Ticker"])

    @patch.object(StockDataTransformer, '_read_and_concatenate')
    @patch.object(StockDataTransformer, '_save_to_csv')
    def test_transform_empty_input(self, mock_save, mock_read):
        def mock_empty():
            return pd.DataFrame(columns=["Open", "High", "Low", "Close", "Adj Close", "Volume"])
        mock_read.side_effect = mock_empty
        transformer = StockDataTransformer(self.tickers, self.extraction_date_str, self.config, self.common_config)
        with self.assertRaises(Exception):
            transformer.transform()
