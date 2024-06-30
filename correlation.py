import os
import pandas as pd
import numpy as np
import posix_ipc
import mmap
import shutil
from datetime import timezone, datetime

from config import LOG_PATH, CORR_NAME, CORR_PATH, MTRX_PATH, MTRX_NAME, COLUMNS, ROWS


def logging(msg, log_name='log_corr.txt'):
    cur_time = datetime.now(timezone.utc)
    with open(os.path.join(LOG_PATH, log_name), 'a') as f:
        f.write(f'[{cur_time}] '+msg)
    print(f'[{cur_time}] '+msg)

def calculate_column_wise_pearson(df):
    correlation_df = df.corr(method='pearson')
    return correlation_df

def automatic_write(data_list, name_list):
    '''
    check log to decide if rollback to last version.
    if the last row is not complete, ignore it.
    if the last row is backup, rollback to last version.
    if the last row is validate, the current files are valid.
    '''
    ori_name_list = []
    temp_name_list = []
    if len(data_list) != len(name_list):
        raise ValueError("===autom write=== : data_list and name_list should have the same length")
    length = len(data_list)
    for i in range(length):
        temp_path = name_list[i]+'.tmp'
        ori_path = name_list[i]+'.bak'
        temp_name_list.append(temp_path)
        ori_name_list.append(ori_path)
        data_list[i].to_csv(temp_path, index=True)
        os.chmod(temp_path, 0o744)
        if os.path.exists(name_list[i]):
            os.rename(name_list[i], ori_path)
            os.chmod(ori_path, 0o744)
    logging('===autom write=== : backup')
    for i in range(length):
        os.rename(temp_name_list[i], name_list[i])
        os.chmod(name_list[i], 0o744)
    logging('===autom write=== : validate')

def rollback(name_list=[CORR_PATH + CORR_NAME, MTRX_PATH + MTRX_NAME]):
    with open(os.path.join(LOG_PATH, 'log_corr.txt'), 'r') as f:
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
    try:
        # shared memory creation.
        # websocket connection
        memory = posix_ipc.SharedMemory("/matrix_memory")
        semaphore_write = posix_ipc.Semaphore("/matrix_write", posix_ipc.O_CREAT)
        semaphore_read = posix_ipc.Semaphore("/matrix_read", posix_ipc.O_CREAT)
        mem_map = mmap.mmap(memory.fd, memory.size)
        LOOP_TIME = 10
        while LOOP_TIME:
            # 拿信号量。
            logging("===Program2=== : Waiting for read semaphore")
            semaphore_read.acquire()
            logging("===Program2=== : Got read semaphore")
            mem_map.seek(0)
            matrix = np.frombuffer(mem_map.read(ROWS * COLUMNS * 8), dtype=np.float64).reshape((ROWS, COLUMNS))
            semaphore_write.release()
            df = pd.DataFrame(matrix, columns=[f"Column{i}" for i in range(COLUMNS)])
            logging("===Program2=== : Correlation matrix calculating...")
            correlation_df = calculate_column_wise_pearson(df)
            logging("===Program2=== : Correlation matrix calculated")
            automatic_write([correlation_df, df], [os.path.join(CORR_PATH, CORR_NAME), os.path.join(MTRX_PATH, MTRX_NAME)])
            logging("===Program2=== : Files updated")
            LOOP_TIME -= 1
    finally:
        pass


if __name__ == "__main__":
    Listen_Regression()



'''
如果两个程序在不同机器上运行，需要用websocket的方式替换共享内存。来通信；
如果矩阵维度非常大，可以考虑用多线程的方式来计算，但是需要考虑线程安全问题；
如果计算相关性太慢，可以考虑用GPU加速计算；
如果计算相关性太慢，可以考虑用近似算法来计算；
如果计算相关性太慢，可以考虑用分布式计算的方式来计算；
如果计算相关性太慢，可以考虑用锁机制或者消息队列的形式平衡两个程序的速度；
'''
