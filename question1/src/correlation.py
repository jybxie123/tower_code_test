import os
import shutil
import sys
import pandas as pd
from pearson import multiple_columns_pearson_correlation_with_threads
from datetime import datetime
from config import LOG_PATH, LOG_NAME, CORR_NAME, CORR_PATH, MTRX_PATH, MTRX_NAME, \
TRANSFER_FORMAT, CORR_PID_FILE, logging
from process_lock import acquire_pid_file, release_pid_file
from transfer import TransferFactory

def calculate_column_wise_pearson(df):
    correlation_df = df.corr(method='pearson')
    return correlation_df

def calculate_column_wise_pearson_optimized(df):
    data = df.to_numpy()
    res = multiple_columns_pearson_correlation_with_threads(data)
    return pd.DataFrame(res, index=df.columns, columns=df.columns)

def automatic_write(data_list, name_list):
    '''
    check log to decide if rollback to last version.
    if the last row is not complete, ignore it.
    if the last row is backup, rollback to last version.
    if the last row is validate, the current files are valid.
    '''
    temp_name_list = []
    if len(data_list) != len(name_list):
        raise ValueError("===autom write=== : data_list and name_list should have the same length")
    length = len(data_list)
    for i in range(length):
        temp_path = name_list[i]+'.tmp'
        ori_path = name_list[i]+'.bak'
        temp_name_list.append(temp_path)
        os.makedirs(os.path.dirname(temp_path), exist_ok=True)
        data_list[i].to_csv(temp_path, index=False)
        os.chmod(temp_path, 0o744)
        if os.path.exists(name_list[i]):
            os.rename(name_list[i], ori_path)
            os.chmod(ori_path, 0o744)
    logging.info('===autom write=== : backup')
    for i in range(length):
        os.rename(temp_name_list[i], name_list[i])
        os.chmod(name_list[i], 0o744)
    logging.info('===autom write=== : validate')

def rollback(name_list=[CORR_PATH + CORR_NAME, MTRX_PATH + MTRX_NAME]):
    with open(os.path.join(LOG_PATH, LOG_NAME), 'r') as f:
        lines = f.readlines()
    if not lines:
        return
    last_complete_line = None
    for line in reversed(lines):
        if '===autom write===' not in line:
            continue
        if line.endswith('\n'):
            last_complete_line = line.strip()
            break
    if not last_complete_line:
        print('no complete log found, should restart.')
    if 'backup' in last_complete_line:
        for name in name_list:
            ori_name = os.path.join(name, '.bak')
            shutil.copy2(ori_name, name)
    elif 'validate' in last_complete_line:
        for name in name_list:
            if os.path.exists(name):
                continue
            if not os.path.exists(name+'.bak'):
                raise FileNotFoundError(f'{name}.bak not found')
            shutil.copy2(name+'.bak', name)
    print('files are rollbacked to last version, valid again.')
    return 

def Listen_Regression():
    if not acquire_pid_file(CORR_PID_FILE):
        return
    try:
        fact = TransferFactory()
        trans = fact.create_transfer_method(TRANSFER_FORMAT[1])
        trans.corr_init()
        LOOP_TIME = 10
        while LOOP_TIME:
            logging.info("===Program 2=== : Waiting for data...")
            df = trans.receive_message()

            logging.info("===Program 2=== : Correlation matrix calculating...")
            start = datetime.now()
            correlation_df = calculate_column_wise_pearson(df)
            delta = datetime.now() - start
            logging.info(f"===Program 2=== : Correlation matrix calculated. Calculation time : {delta.total_seconds()}")
            
            logging.info("===Program 2=== : Optimized Correlation matrix calculating...")
            start2 = datetime.now()
            correlation_df2 = calculate_column_wise_pearson_optimized(df)
            delta2 = datetime.now() - start2
            logging.info(f"===Program 2=== : Optimized Correlation matrix calculated. Calculation time : {delta2.total_seconds()}")
            
            print(f"===Program 2=== : Correlation matrix========")
            print(correlation_df.head(10))
            print(f"===Program 2=== : Optimized Correlation matrix========")
            print(correlation_df2.head(10))
            
            automatic_write([correlation_df, df], [os.path.join(CORR_PATH, CORR_NAME), os.path.join(MTRX_PATH, MTRX_NAME)])
            logging.info("===Program 2=== : Files updated")
            LOOP_TIME -= 1
    except TimeoutError:
        logging.info("===Program 2=== : Timeout, finished.")
    finally:
        release_pid_file(CORR_PID_FILE)


if __name__ == "__main__":
    Listen_Regression()

