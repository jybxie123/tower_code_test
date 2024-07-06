import time
import pandas as pd
import numpy as np
import os
from datetime import datetime
from transfer import TransferFactory
from process_lock import acquire_pid_file, release_pid_file
from config import COLUMNS, ROWS, MTRX_PATH, MTRX_NAME, TRANSFER_FORMAT, MTRX_PID_FILE, logging

def initialize_matrix():
    if os.path.exists(os.path.join(MTRX_PATH, MTRX_NAME)):
        df = pd.read_csv(os.path.join(MTRX_PATH, MTRX_NAME))
    else:
        df = pd.DataFrame(np.random.randn(ROWS, COLUMNS), columns=[f'Column{i}' for i in range(COLUMNS)], index=range(ROWS))
    return df

def update_matrix(df):
    num_cells_to_update = (ROWS * COLUMNS) // 2
    indices_to_update = np.random.choice(range(ROWS * COLUMNS), num_cells_to_update, replace=False)
    random_values = np.random.randn(num_cells_to_update)
    for i, index in enumerate(indices_to_update):
        df.iat[index // COLUMNS, index % COLUMNS] = random_values[i]
    return df

def run():
    if not acquire_pid_file(MTRX_PID_FILE):
        return
    df = initialize_matrix()
    fact = TransferFactory()
    trans = fact.create_transfer_method(TRANSFER_FORMAT[1])
    LOOP_TIME = 10
    trans.mtrx_init()
    try:
        while LOOP_TIME:
            logging.info('===Program 1=== : start to update the matrix...')
            update_start_time = datetime.now()
            df = update_matrix(df)
            update_end_time = datetime.now()
            logging.info(f'===Program 1=== : update time : {(update_end_time - update_start_time).total_seconds()}')

            trans.send_message(df)
            
            sleeptime = np.random.randint(1, 6)
            logging.info(f'===Program 1=== : sleep for {sleeptime} seconds.')
            time.sleep(sleeptime)
            
            LOOP_TIME -= 1
    finally:
        trans.final_mtrx()
        release_pid_file(MTRX_PID_FILE)

if __name__ == '__main__':
    run()

