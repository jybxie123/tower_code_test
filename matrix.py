import time
import pandas as pd
import numpy as np
import os
import posix_ipc
import mmap
from datetime import timezone, datetime

# import the global variables from correlation.py, just means to avoid data inconsistency
from correlation import COLUMNS, ROWS, MTRX_PATH, MTRX_NAME, LOG_PATH



def logging(msg, log_name='log_mtrx.txt'):
    cur_time = datetime.now(timezone.utc)
    with open(os.path.join(LOG_PATH, log_name), 'a') as f:
        f.write(f'[{cur_time}] '+msg)
    print(f'[{cur_time}] '+msg)

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
    LOOP_TIME = 10
    df = initialize_matrix()
    # 计算 DataFrame 转换为字节后的大小
    array = df.to_numpy()
    memory_size = array.size * array.itemsize  # 计算字节数
    logging(f"Memory size needed: {memory_size} bytes")
    memory = posix_ipc.SharedMemory("/matrix_memory", posix_ipc.O_CREAT, size=ROWS * COLUMNS * 8) #
    semaphore_write = posix_ipc.Semaphore("/matrix_write", posix_ipc.O_CREAT, initial_value=1)
    semaphore_read = posix_ipc.Semaphore("/matrix_read", posix_ipc.O_CREAT, initial_value=0)
    mem_map = mmap.mmap(memory.fd, memory.size)
    try:
        while LOOP_TIME:
            logging('===Program 1=== : start to update the matrix...\n')
            update_start_time = datetime.now()
            df = update_matrix(df)
            update_end_time = datetime.now()
            logging(f'===Program 1=== : update time : {(update_end_time - update_start_time).total_seconds()}\n')
            semaphore_write.acquire()
            # if not semaphore_write.acquire(timeout=0.1):
            #     logging(f'===Program 1=== : acquire the semaphore timeout.\n')
            #     continue
            logging(f'===Program 1=== : acquire the semaphore.\n')
            mem_map.seek(0)
            mem_map.write(df.to_numpy().tobytes())
            semaphore_read.release()
            logging(f'===Program 1=== : release the semaphore.\n')
            sleeptime = np.random.randint(1, 6)
            logging(f'===Program 1=== : sleep for {sleeptime} seconds.\n')
            time.sleep(sleeptime)
            LOOP_TIME -= 1
    finally:
        mem_map.close()
        memory.close_fd()
        memory.unlink()
        semaphore_read.unlink()
        semaphore_write.unlink()

if __name__ == '__main__':
    # Generate random data
    run()

