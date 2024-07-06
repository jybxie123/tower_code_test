import os

ROOT_PATH = "/Users/jiangyanbo/working/code_exercise/tower/question2"
if not os.path.exists(ROOT_PATH):
    raise Exception(f"Root path {ROOT_PATH} does not exist, please check the path in config.py")
DATA_NAME = "data/test_data.csv"
TICKERS = ['AAPL', 'IBM', 'GOOG', 'MSFT']
MARKET = "market"
DATE = "date"
LOG_NAME = "log/return_beta.log"
