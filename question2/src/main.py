import pandas as pd
import numpy as np
from datetime import datetime
import os
from return_stra import PandasStrategy as PdReturn, PandasGroupStrategy as PdGpReturn, \
    NumpyStrategy as NpReturn, NumpyMultiProcStrategy as NpMultiProcReturn
from config import TICKERS, ROOT_PATH, DATA_NAME, MARKET, DATE
from beta import PandasStrategy as PdBeta, NumpyStrategy as NpBeta, OptimizedNumpyStrategy as OpNpBeta

RETURNSTRATEGYDICT = {
    'pandas': PdReturn(),
    'pandas_group': PdGpReturn(),
    'numpy': NpReturn(),
    'numpy_multiproc': NpMultiProcReturn(),
}

BETASTRATEGYDICT = {
    'pandas': PdBeta(),
    'numpy': NpBeta(),
    'numpy_optimized': OpNpBeta(),
}

def get_origin_data():
    data_path = os.path.join(ROOT_PATH, DATA_NAME)
    if os.path.exists(data_path):
        df = pd.read_csv(data_path)
        df[DATE] = pd.to_datetime(df[DATE], format='%Y%m%d')
        return df
    dates = pd.date_range(start='2022-01-01', end='2022-12-31', freq='B') 
    data = []
    for date in dates:
        for ticker in TICKERS:
            ret = np.random.normal(0.02, 0.1)
            weight = np.random.uniform(0.1, 0.5)
            data.append([date.strftime('%Y%m%d'), ticker, ret, weight])
    df = pd.DataFrame(data, columns=[DATE, 'ticker', 'return', 'weight'])
    df.to_csv(data_path, index=False)
    df[DATE] = pd.to_datetime(df[DATE], format='%Y%m%d')
    return df

def get_all_return(df, market_return):
    df_pivot = df.pivot_table(index=DATE, columns='ticker', values='return')
    df_pivot[MARKET] = market_return
    return df_pivot



'''
可优化：
    return部分：
    计算市场的时候，可以pandas或者numpy的groupby方法，提高效率。
    多进程实现？似乎不用。


    beta部分：
    复用计算结果，中间计算值改为属性记录下来，每次修改日期时，只需更新部分数据。
    使用numpy计算，提高计算效率。
    并行计算。
'''


if __name__ == '__main__':

    df = get_origin_data()
    for i in RETURNSTRATEGYDICT:
        market_return = RETURNSTRATEGYDICT[i].get_return(df)
        # print(i)
        # print(market_return)
    
    tickers = get_all_return(df, market_return)
    # print(tickers)

    for i in BETASTRATEGYDICT:
        start = datetime.now()
        beta_df = BETASTRATEGYDICT[i].beta(tickers)
        delta = datetime.now() - start
        print(f"{i} Beta calculation time: {delta.total_seconds()} seconds")
        print(beta_df.head(10))



