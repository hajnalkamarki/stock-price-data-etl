# stock-price-data-etl

An object-oriented ETL (Extract, Transform, Load) pipeline for Yahoo Finance stock price data, with modular components, CLI, and test coverage.

## Features
- Extracts historical stock data for configurable tickers and date ranges using yfinance
- Transforms data with cleaning, rolling averages, daily returns, EMA, and crossover detection
- Loads and aggregates transformed data for further analysis
- Config-driven operation (YAML)
- MultiIndex DataFrames (Date, Ticker)
- Command-line interface (CLI) for all ETL steps
- Unit tests for all major components (extractor, transformer, loader)

## Project Structure

```
stock-price-data-etl/
├── main.py
├── README.md
├── config.yaml
├── yahoo_stock_price_etl/
│   ├── __init__.py
│   ├── extractor.py
│   ├── transformer.py
│   └── loader.py
└── tests/unit/
    ├── test_extractor_unit.py
    ├── test_transformer_unit.py
    └── ...
```

## Setup

1. **Clone the repository:**
   ```sh
   git clone https://github.com/hajnalkamarki/stock-price-data-etl.git
   cd stock-price-data-etl
   ```

2. **Install dependencies:**
   ```sh
   pip install -r requirements.txt
   ```
   Or install manually:
   ```sh
   pip install yfinance pandas typer pytest
   ```

3. **Configure your ETL run:**
   Edit `config.yaml` to set tickers, date ranges, file paths, and transform parameters.

## Usage

Run the ETL pipeline from the command line:

```sh
python ./app.py extract --tickers AAPL,GOOG --start "2020-02-01" --end "2020-04-02"
python main.py transform --tickers AAPL,GOOG  --extraction-date-str "2025_07_20"
python main.py load --extraction-date-str "2025_07_20"
```

Or use default configurations from the config file as are parameters are optional:

```sh
python ./app.py extract
python main.py transform
python main.py load
```

## Testing

Run a specific test file:

```sh
python -m unittest tests/test_transformer.py 
```

## Extending

- Add new features by extending the ETL classes in `yahoo_stock_price_etl/`
- Add new tests in the `tests/` directory

## License

MIT License