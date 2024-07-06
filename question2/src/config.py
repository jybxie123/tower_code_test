import logging
import os

ROOT_PATH = "/Users/jiangyanbo/working/code_exercise/tower/question2"
DATA_NAME = "data/test_data.csv"
TICKERS = ['AAPL', 'IBM', 'GOOG', 'MSFT']
MARKET = "market"
DATE = "date"
LOG_NAME = "log/return_beta.log"

logging.basicConfig(filename=os.path.join(ROOT_PATH, LOG_NAME),
                    level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')
