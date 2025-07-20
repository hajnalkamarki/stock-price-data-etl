import unittest
import tempfile
import pandas as pd
from yahoo_stock_price_etl.extractor import StockDataExtractor


class TestStockDataExtractor(unittest.TestCase):
    def setUp(self):
        self.tmp_dir = tempfile.TemporaryDirectory()
        self.tickers = ["AAPL"]
        self.start = "2023-01-01"
        self.end = "2023-01-10"
        self.common_config = {"file_path": self.tmp_dir.name}

    def tearDown(self):
        self.tmp_dir.cleanup()

    def test_extract_multiple_tickers(self):
        tickers = ["AAPL", "MSFT"]
        extractor = StockDataExtractor(tickers, self.start, self.end, common_config=self.common_config)
        df = extractor.extract()
        self.assertIsInstance(df, pd.DataFrame)
        self.assertIn("Close", df.columns)
        # Should contain at least one of the tickers in the data (if yfinance returns data)
        if not df.empty and "Ticker" in df.columns:
            self.assertTrue(df["Ticker"].isin(tickers).any())

    def test_extract_date_range(self):
        extractor = StockDataExtractor(self.tickers, self.start, self.end, common_config=self.common_config)
        df = extractor.extract()
        self.assertIsInstance(df, pd.DataFrame)
        if not df.empty:
            self.assertGreaterEqual(df["Date"].min(), pd.to_datetime(self.start))
            self.assertLessEqual(df["Date"].max(), pd.to_datetime(self.end))

    def test_extract_dataframe_columns(self):
        extractor = StockDataExtractor(self.tickers, self.start, self.end, common_config=self.common_config)
        df = extractor.extract()
        expected_cols = {"Date", "Open", "High", "Low", "Close", "Volume"}
        self.assertTrue(expected_cols.issubset(set(df.columns)))

    def test_extract_empty_for_future_dates(self):
        future_start = "2100-01-01"
        future_end = "2100-01-10"
        extractor = StockDataExtractor(self.tickers, future_start, future_end, common_config=self.common_config)
        df = extractor.extract()
        self.assertIsInstance(df, pd.DataFrame)
        self.assertTrue(df.empty)

    def test_extract_dataframe_shape(self):
        extractor = StockDataExtractor(self.tickers, self.start, self.end, common_config=self.common_config)
        df = extractor.extract()
        if not df.empty:
            self.assertGreaterEqual(len(df), 1)
            self.assertGreaterEqual(len(df.columns), 6)

    def test_extract_with_invalid_ticker(self):
        extractor = StockDataExtractor(["INVALIDTICKER"], self.start, self.end, common_config=self.common_config)
        df = extractor.extract()
        # Should return an empty DataFrame or DataFrame with no rows
        self.assertIsInstance(df, pd.DataFrame)
        self.assertTrue(df.empty or len(df) == 0)
