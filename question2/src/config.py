import os

ROOT_PATH = "/your/path/to/tower_code_test/question2"
if not os.path.exists(ROOT_PATH):
    raise Exception(f"Root path {ROOT_PATH} does not exist, please check the path in config.py")
DATA_NAME = "data/test_data.csv"
TICKERS = ['AAPL', 'IBM', 'GOOG', 'MSFT']
MARKET = "market"
DATE = "date"
