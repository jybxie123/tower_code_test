import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os
from abc import ABC, abstractmethod
from multiprocessing import Pool, cpu_count

class ReturnStrategy(ABC):
    @abstractmethod
    def get_return(self, df):
        pass


class PandasStrategy(ReturnStrategy):
    def get_return(self, df):
        dates = pd.date_range(start='2022-01-01', end='2022-12-31', freq='B') 
        market_return = {}
        df['weighted_return'] = df['return'] * df['weight']
        for date in dates:
            # print(type(date), df['date'].dtype, 3)
            df_date = df.query('date==@date')
            nominator = df_date['weighted_return'].sum()
            denominator = df_date['weight'].sum()
            market_return[date] = nominator / denominator if denominator else 0.0
        df = df.drop('weighted_return', axis=1)
        return pd.Series(market_return)


class PandasGroupStrategy(ReturnStrategy):
    def get_return(self, df):
        df['weighted_return'] = df['return'] * df['weight']
        return df.groupby('date').apply(lambda x: x['weighted_return'].sum() / x['weight'].sum())


class NumpyStrategy(ReturnStrategy):
    def get_return(self, df):
        data = df.to_numpy()
        market_return = {}
        dates = pd.date_range(start='2022-01-01', end='2022-12-31', freq='B')
        
        for date in dates:
            mask = data[:,0] == date
            nominator = np.sum(data[mask, 2]*data[mask, 3])
            denominator = np.sum(data[mask, 3])
            market_return[date] = nominator / denominator if denominator else 0.0
        return pd.Series(market_return) 


class NumpyMultiProcStrategy(ReturnStrategy):
    def get_return(self, df):
        data = df.to_numpy()
        market_return = {}
        dates = pd.date_range(start='2022-01-01', end='2022-12-31', freq='B')
        with Pool(cpu_count()//2) as pool:
            results = pool.map(self.calculate_return, [(date, data) for date in dates])
        market_return = dict(results)
        return pd.Series(market_return)

    def calculate_return(self, args):
        date, data = args
        mask = data[:,0] == date
        nominator = np.sum(data[mask, 2] * data[mask, 3])
        denominator = np.sum(data[mask, 3])
        return date, nominator / denominator if denominator else 0.0



