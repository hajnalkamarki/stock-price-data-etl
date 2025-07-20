import typer
import yaml
from datetime import datetime
from typing import List
from yahoo_stock_price_etl.extractor import StockDataExtractor
from yahoo_stock_price_etl.transformer import StockDataTransformer
from yahoo_stock_price_etl.loader import StockDataLoader

app = typer.Typer()

with open("config.yaml", "r") as f:
    APP_CONFIG = yaml.safe_load(f)

@app.command()
def extract(
    tickers: List[str] = typer.Option(APP_CONFIG['yahoo_stock_price_etl']['extractor']['tickers'], help="Stock ticker symbol"),
    start: str = typer.Option(APP_CONFIG['yahoo_stock_price_etl']['extractor']['start_date'], help="Start date (YYYY-MM-DD)"),
    end: str = typer.Option(APP_CONFIG['yahoo_stock_price_etl']['extractor']['end_date'], help="End date (YYYY-MM-DD)")
) -> StockDataExtractor:
    """Extract stock data from Yahoo Finance."""
    common_config = APP_CONFIG['yahoo_stock_price_etl'].get('common', {})
    extractor = StockDataExtractor(
        tickers=tickers, 
        start=start, 
        end=end, 
        common_config=common_config
    )
    extractor.extract()

    return extractor

@app.command()
def transform(
    tickers: List[str] = typer.Option(APP_CONFIG['yahoo_stock_price_etl']['extractor']['tickers'], help="Stock ticker symbol"),
    extraction_date_str: str = typer.Option(datetime.now().strftime("%Y_%m_%d"), help="Processing date (YYYY_MM_DD)"),
):
    """Clean and transform stock data."""
    common_config = APP_CONFIG['yahoo_stock_price_etl'].get('common', {})
    transformer = StockDataTransformer(
        tickers=tickers, 
        extraction_date_str=extraction_date_str, 
        config=APP_CONFIG['yahoo_stock_price_etl']['transformer'],
        common_config=common_config
    )
    transformer.transform()

@app.command()
def load(
    extraction_date_str: str = typer.Option(datetime.now().strftime("%Y_%m_%d"), help="Processing date (YYYY_MM_DD)"),
):
    """Load stock data."""
    common_config = APP_CONFIG['yahoo_stock_price_etl'].get('common', {})
    loader = StockDataLoader(
        extraction_date_str=extraction_date_str, 
        config=APP_CONFIG['yahoo_stock_price_etl']['transformer'],
        common_config=common_config)
    loader.load()

@app.command()
def etl(
    tickers: List[str] = typer.Option(APP_CONFIG['yahoo_stock_price_etl']['extractor']['tickers'], help="Stock ticker symbol"),
    start: str = typer.Option(APP_CONFIG['yahoo_stock_price_etl']['extractor']['start_date'], help="Start date (YYYY-MM-DD)"),
    end: str = typer.Option(APP_CONFIG['yahoo_stock_price_etl']['extractor']['end_date'], help="End date (YYYY-MM-DD)")
):
    """Run the full ETL pipeline."""
    extraction_date_str: str = extract(tickers=tickers, start=start, end=end).day_str
    transform(tickers=tickers, extraction_date_str=extraction_date_str)
    load(extraction_date_str=extraction_date_str)

if __name__ == "__main__":
    app()
