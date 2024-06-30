import time
import pandas as pd
import numpy as np
import os
import posix_ipc
import mmap
from datetime import timezone, datetime
from connection import TransferFactory

# import the global variables from correlation.py, just means to avoid data inconsistency
from config import COLUMNS, ROWS, MTRX_PATH, MTRX_NAME, TRANSFER_FORMAT, logging

def initialize_matrix():
    if os.path.exists(os.path.join(MTRX_PATH, MTRX_NAME)):
        df = pd.read_csv(os.path.join(MTRX_PATH, MTRX_NAME))
    else:
        df = pd.DataFrame(np.random.randn(ROWS, COLUMNS), columns=[f"col_{i}" for i in range(COLUMNS)], index=range(ROWS))
        df.to_csv(os.path.join(MTRX_PATH, MTRX_NAME))
    return df

def update_matrix(df):
    num_cells_to_update = (ROWS * COLUMNS) // 2
    indices_to_update = np.random.choice(range(ROWS * COLUMNS), num_cells_to_update)
    for index in indices_to_update:
        row = index // ROWS
        col = index % COLUMNS
        df.iat[row, col] = np.random.randn()
    df.to_csv(os.path.join(MTRX_PATH, MTRX_NAME))
    return df

def update_matrix(df):
    num_cells_to_update = (ROWS * COLUMNS) // 2
    indices_to_update = np.random.choice(range(ROWS * COLUMNS), num_cells_to_update, replace=False)
    random_values = np.random.randn(num_cells_to_update)
    for i, index in enumerate(indices_to_update):
        df.iat[index // COLUMNS, index % COLUMNS] = random_values[i]
    df.to_csv(os.path.join(MTRX_PATH, MTRX_NAME), index=False)
    return df

def run():
    df = initialize_matrix()
    # 计算 DataFrame 转换为字节后的大小
    array = df.to_numpy()
    memory_size = array.size * array.itemsize  # 计算字节数
    logging.info(f"Memory size needed: {memory_size} bytes")
    fact = TransferFactory()
    trans = fact.create_transfer_method(TRANSFER_FORMAT[1], shm_size=memory_size)
    LOOP_TIME = 10
    trans.mtrx_init()
    # memory = posix_ipc.SharedMemory("/matrix_memory", posix_ipc.O_CREAT, size=800000) #
    # semaphore_write = posix_ipc.Semaphore("/matrix_write", posix_ipc.O_CREAT, initial_value=1)
    # semaphore_read = posix_ipc.Semaphore("/matrix_read", posix_ipc.O_CREAT, initial_value=0)
    # mem_map = mmap.mmap(memory.fd, memory.size)
    try:
        while LOOP_TIME:
            logging.info('===Program 1=== : start to update the matrix...')
            update_start_time = datetime.now()
            df = update_matrix(df)
            update_end_time = datetime.now()
            logging.info(f'===Program 1=== : update time : {(update_end_time - update_start_time).total_seconds()}')
            trans.send_message(df)
            # semaphore_write.acquire()
            # logging.info(f'===Program 1=== : acquire the semaphore.')
            # mem_map.seek(0)
            # mem_map.write(df.to_numpy().tobytes())
            # semaphore_read.release()
            logging.info(f'===Program 1=== : release the semaphore.')
            sleeptime = np.random.randint(1, 6)
            logging.info(f'===Program 1=== : sleep for {sleeptime} seconds.')
            time.sleep(sleeptime)
            LOOP_TIME -= 1
    finally:
        trans.final_mtrx()

if __name__ == '__main__':
    # Generate random data
    run()

