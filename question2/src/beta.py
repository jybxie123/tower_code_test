import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os
from abc import ABC, abstractmethod
from config import TICKERS, MARKET, DATE
from cal_beta import cal_beta


class BetaStrategy(ABC):
    '''
    This class accept the ticker_df as its input.
    '''
    @abstractmethod
    def beta(self, df):
        pass


class PandasStrategy(BetaStrategy):
    def beta(self, ori_df):
        dates = pd.date_range(start='2022-01-01', end='2022-12-31', freq='B') 
        beta = {}
        for date in dates:
            date_str = date.strftime('%Y%m%d')
            end_date = date - timedelta(days=1)
            start_date = (end_date - timedelta(days=127)) if end_date - timedelta(days=127) > datetime(2022, 1, 1) else datetime(2022, 1, 1)
            df = ori_df.loc[start_date.strftime('%Y%m%d'):end_date.strftime('%Y%m%d')]
            x = MARKET
            ticker_beta = {}
            x_mean = df[x].mean()
            x_var = (df[x] - x_mean) ** 2
            for y in TICKERS:
                y_mean = df[y].mean()
                xy_cov = (df[x] - x_mean) * (df[y] - y_mean)
                ticker_beta[y] = xy_cov.sum() / x_var.sum() if x_var.sum() else 0
            beta[date_str] = ticker_beta
        beta_df = pd.DataFrame(beta).fillna(0.0).T
        return beta_df


class NumpyStrategy(BetaStrategy):
    def beta(self, df):
        df = df.round(6)
        dates = pd.date_range(start='2022-01-01', end='2022-12-31', freq='B') 
        beta = {}
        all_x = df[MARKET].values
        data = df.reset_index()[[DATE]+TICKERS].to_numpy()
        data[:, 0] = pd.to_datetime(data[:, 0], format='%Y%m%d')
        num = 0
        for end_date in dates:
            start_date = (end_date - timedelta(days=128)) if end_date - timedelta(days=128) > datetime(2022, 1, 1) else datetime(2022, 1, 1)
            mask = (start_date <= data[:,0]) & (data[:,0] < end_date)
            ticker_beta = {}
            x = all_x[mask]
            x_mean = np.mean(x) if len(x) != 0 else 0
            denominator = np.sum((x - x_mean) ** 2)
            for ticker in range(data.shape[1]-1):
                y = data[mask, ticker+1]
                y_mean = np.mean(y) if len(y) != 0 else 0
                numerator = np.sum((x - x_mean) * (y - y_mean))
                ticker_beta[ticker] = numerator / denominator if denominator else 0.0
            beta[end_date] = ticker_beta
            num += 1
        df = pd.DataFrame(beta).T
        df.columns = TICKERS
        return df


class OptimizedNumpyStrategy(BetaStrategy):
    def beta(self, df):
        df = df.reset_index()
        df[DATE] = df[DATE].apply(lambda x: x.timestamp())
        data = df[[DATE]+TICKERS+[MARKET]].to_numpy()
        res = cal_beta(data, TICKERS)
        df = pd.DataFrame(res, columns=[DATE]+TICKERS)
        df[DATE] = pd.to_datetime(df[DATE], unit='s')
        df.set_index(DATE, inplace=True)
        return df


