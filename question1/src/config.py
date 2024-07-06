import os
import logging

ROOT_PATH = '/your/path/to/tower_code_test/question1'
if not os.path.exists(ROOT_PATH):
    raise Exception(f"Root path {ROOT_PATH} does not exist, please check the path in config.py")
LOG_PATH = os.path.join(ROOT_PATH, 'log')
CORR_PATH = os.path.join(ROOT_PATH, 'data')
MTRX_PATH = os.path.join(ROOT_PATH, 'data')
LOG_NAME = 'matrix_correlation.log'
CORR_NAME = "correlation_matrix.csv"
MTRX_NAME = 'updated_matrix.csv'
COLUMNS = 1000
ROWS = 10000
TIMEOUT = 60

SHARED_MEMORY_NAME = "/matrix_memory"
SEMAPHORE_WRITE = "/matrix_write"
SEMAPHORE_READ = "/matrix_read"
SHARED_MEMORY = (ROWS) * COLUMNS * 8

WS_HOST = "localhost"
WS_PORT = 12345
WEBSOCKET_MAX_SIZE = ROWS * COLUMNS * 1000 

MTRX_PID_FILE = 'mtrx_program.pid'
CORR_PID_FILE = 'corr_program.pid'
TRANSFER_FORMAT = ['websocket', 'shared_memory']

if os.path.exists(ROOT_PATH):
    os.makedirs(LOG_PATH, exist_ok=True)
logging.basicConfig(filename=os.path.join(LOG_PATH, LOG_NAME),
                    level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')



