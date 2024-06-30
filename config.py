import os
from datetime import datetime, timezone
import logging

ROOT_PATH = '/Users/jiangyanbo/working/code_exercise/tower/question1'
LOG_PATH = os.path.join(ROOT_PATH, 'code')
CORR_PATH = os.path.join(ROOT_PATH, 'code')
MTRX_PATH = os.path.join(ROOT_PATH, 'code')
LOG_NAME = 'matrix_correlation.log'
CORR_NAME = "correlation_matrix.csv"
MTRX_NAME = 'updated_matrix.csv'
COLUMNS = 1000
ROWS = 100
SHARED_MEMORY_NAME = "/matrix_memory"
SEMAPHORE_WRITE = "/matrix_write"
SEMAPHORE_READ = "/matrix_read"
WS_HOST = "localhost"
WS_PORT = 12345

TRANSFER_FORMAT = ['websocket', 'shared_memory']

logging.basicConfig(filename=os.path.join(LOG_PATH, LOG_NAME),
                    level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')



