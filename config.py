import os
ROOT_PATH = '/Users/jiangyanbo/working/code_exercise/tower/question1'
LOG_PATH = os.path.join(ROOT_PATH, 'code')
CORR_PATH = os.path.join(ROOT_PATH, 'code')
MTRX_PATH = os.path.join(ROOT_PATH, 'code')
CORR_NAME = "correlation_matrix.csv"
MTRX_NAME = 'updated_matrix.csv'
COLUMNS = 1000
ROWS = 100
SHARED_MEMORY_NAME = "/matrix_memory"
SEMAPHORE_WRITE = "/matrix_write"
SEMAPHORE_READ = "/matrix_read"
WS_HOST = "localhost"
WS_PORT = 12345


