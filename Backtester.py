import pandas as pd
import numpy as np
from DataScraper import *
import matplotlib.pyplot as plt

class Backtester:
    def __init__(self, method='c_25%', RA=1, RE=1, TC=0.01, v_filter_period=14, start_date=pd.to_datetime("2020-09-11"), offset=0, isDOWN=False): # 25%는 고정! 하드코딩!
        self.method = method    #'c_25%'
        self.RA = RA    # 관찰 기간
        self.RE = RE    # 리밸런싱 주기
        self.TC = TC    # 거래 수수료
        self.v_filter_period = v_filter_period  # 거래량 필터 관찰기간
        self.start_date = start_date
        self.offset = offset
        self.isDOWN = isDOWN
        self.cumrr_df = pd.DataFrame()
        self.filter_list = ['c_nan_filter', 'v_filter', 'rr_filter', ''] # 이 순서를 지키도록!

        self.ds = DataScraper(isFromFile=True, isDOWN=isDOWN)
        self.ohlcv_dic = self.ds.usdt_ohlcv_dic

        self.c_df, self.v_df, self.c_nan_filter = self.get_dfs(self.ohlcv_dic, self.start_date)
        self.v_filter = self.get_v_filter(v_df=self.v_df, v_filter_period=self.v_filter_period)
        self.rr_daily_df = self.get_rr_df(c_df=self.c_df, period=1)

        self.set_strategy(method, RA, RE, TC, offset)

    def set_strategy(self, method, RA, RE, TC, offset):
        self.rr_filter = self.get_rr_filter(c_df=self.c_df, RA=RA, method=method)
        self.total_filter = self.get_total_filter(c_nan_filter=self.c_nan_filter, v_filter=self.v_filter, rr_filter=self.rr_filter)
        self.w_df = self.get_weight_df(rr_daily_df=self.rr_daily_df, filter=self.total_filter, RE=RE, offset=offset)
        self.tr_s = self.get_tr_s(w_df=self.w_df, RE=RE, offset=offset)
        self.rr_port_s = self.get_rr_port_s(w_df=self.w_df, rr_daily_df=self.rr_daily_df, tr_s=self.tr_s, method=method, RA=RA, RE=RE, TC=TC, offset=offset)
        self.cumrr_s = self.plot_crr(rr_port_s=self.rr_port_s)

    def plot_crr(self, rr_port_s):
        rr = rr_port_s + 1
        cumrr_s = rr.cumprod()
        MDD = self.get_MDD(cumrr_s=cumrr_s)
        cumrr_s.name = f"{rr.name}, MDD:{MDD}"
        pd.DataFrame(cumrr_s).plot()
        return cumrr_s

    def get_MDD(self, cumrr_s):
        """
        MDD(Maximum Draw-Down)
        :return: (peak_upper, peak_lower, mdd rate)
        """
        arr_v = np.array(cumrr_s)
        peak_lower = np.argmax(np.maximum.accumulate(arr_v) - arr_v)
        peak_upper = np.argmax(arr_v[:peak_lower])
        return (arr_v[peak_lower] - arr_v[peak_upper]) / arr_v[peak_upper]

    def add_to_crr_df(self, cumrr_s):
        self.cumrr_df = pd.concat([self.cumrr_df, cumrr_s], axis=1, join='outer')
        self.cumrr_df.plot()

    @staticmethod
    def get_dfs(ohlcv_dic, start_date):
        # 리턴할 df 초기화
        c_df = pd.DataFrame()
        v_df = pd.DataFrame()
        # symbol 하나씩 concat
        for symbol in ohlcv_dic.keys():
            ohlcv = ohlcv_dic[symbol]
            close = ohlcv.loc[:, ['datetime', 'close']].set_index('datetime').rename(columns={'close':symbol})
            volume = ohlcv.loc[:, ['datetime', 'volume']].set_index('datetime').rename(columns={'volume':symbol})

            if 0.9 < close.min().min() < 1.1 and 0.9 < close.max().max() < 1.1: # 스테이블 코인은 제외
                print(symbol, "is stable coin.")
                continue
            c_df = pd.concat([c_df, close], axis=1, join='outer')
            v_df = pd.concat([v_df, volume], axis=1, join='outer')
        # 날짜순으로 sorting
        c_df.sort_index()
        v_df.sort_index()

        c_isnan = c_df.isna()
        c_nan_filter = (c_isnan == False)
        c_df = c_df.fillna(method='ffill')
        return c_df.loc[start_date:], v_df.loc[start_date:], c_nan_filter.loc[start_date:]
    @staticmethod
    def get_v_filter(v_df, v_filter_period):
        v_ma_df = v_df.rolling(v_filter_period).mean()
        v_filter = v_ma_df >= 1000 # 이동평균 1000USD 이상
        return v_filter
    @staticmethod
    def get_rr_filter(c_df, RA, method):
        rr_df = Backtester.get_rr_df(c_df, RA)
        rr_df_T = rr_df.T
        if method == 'c_25%':
            rr_threshold = rr_df_T.describe().loc['25%']
            rr_filter = (rr_df.sub(rr_threshold, axis=0) <= 0)
        elif method == 'c_50%':
            rr_threshold = rr_df_T.describe().loc['50%']
            rr_filter = (rr_df.sub(rr_threshold, axis=0) <= 0)
        elif method == 'c_75%':
            rr_threshold = rr_df_T.describe().loc['75%']
            rr_filter = (rr_df.sub(rr_threshold, axis=0) <= 0)
        elif method == 'm_25%':
            rr_threshold = rr_df_T.describe().loc['25%']
            rr_filter = (rr_df.sub(rr_threshold, axis=0) >= 0)
        elif method == 'm_50%':
            rr_threshold = rr_df_T.describe().loc['50%']
            rr_filter = (rr_df.sub(rr_threshold, axis=0) >= 0)
        elif method == 'm_75%':
            rr_threshold = rr_df_T.describe().loc['75%']
            rr_filter = (rr_df.sub(rr_threshold, axis=0) >= 0)
        elif method == '25%~75%':
            rr_threshold25 = rr_df_T.describe().loc['25%']
            rr_threshold75 = rr_df_T.describe().loc['75%']
            rr_filter = (rr_df.sub(rr_threshold25, axis=0) >= 0) & (rr_df.sub(rr_threshold75, axis=0) <= 0)
        elif method =='a':
            rr_filter = pd.DataFrame(data=True, index=c_df.index, columns=c_df.columns)
        return rr_filter
    @staticmethod
    def get_rr_df(c_df, period):
        rr_df = (c_df - c_df.shift(period))/c_df.shift(period)
        return rr_df
    @staticmethod
    def get_total_filter(c_nan_filter, v_filter, rr_filter):
        total_filter = (c_nan_filter & v_filter & rr_filter).shift(periods=1, fill_value=False)
        return total_filter
    @staticmethod
    def get_weight_df(rr_daily_df, filter, RE=1, offset=0):
        if RE == 1:  #매일 리밸런싱
            n_s = filter.sum(axis=1)
            w_df = filter.div(n_s, axis=0)
            w_df = w_df.fillna(value=0)
            return w_df
        w_df = filter.copy() # index 통일을 위해
        is_Rebal = False
        for i, date in enumerate(rr_daily_df.index):
            if (i - offset) % RE == 0: # 리밸런싱데이
                w_df.loc[date] = filter.loc[date] / filter.loc[date].sum()
                sum = w_df.loc[date].sum()
                if sum != 0:  # weight의 합이 0인 아닌 경우 : 매수한게 있는 경우
                    is_Rebal = True
            else: # 리밸런싱 안하는 날
                if is_Rebal: # 리밸런싱 한번 이상 한 경우
                    w_df.loc[date] = (1 + rr_daily_df.iloc[i - 1]) * w_df.iloc[i - 1]
                    sum = w_df.loc[date].sum()
                    w_df.loc[date] = w_df.loc[date] / sum
                else: # 한번도 매수 안한 경우
                    w_df.loc[date] = w_df.iloc[0] * 1
        if w_df.sum(axis=1).max() >= 1.000001:
            print("weight df something wrong!!!!!!")
        return w_df

    @staticmethod
    def get_tr_s(w_df, RE=1, offset=0):
        # tr_s = pd.Series(data=0, index=w_df.index, name='tr')
        tr_df = pd.DataFrame(data=0, index=w_df.index, columns=w_df.columns)
        is_Rebal = False
        for i, date in enumerate(w_df.index):
            if (i - offset) % RE == 0:  # 리밸런싱데이
                if not is_Rebal: # 리밸런싱 한번도 안한 경우
                    sum = w_df.loc[date].sum()
                    if sum != 0:  # weight의 합이 0인 아닌 경우 : 매수한게 있는 경우
                        is_Rebal = True
                        tr_df.loc[date] = w_df.loc[date]
                else: # 리밸런싱 기존에 한번 이상 한 경우
                    tr_df.loc[date] = abs(w_df.loc[date] - w_s_Rebal)
                w_s_Rebal = w_df.iloc[i]
        tr_s = tr_df.sum(axis=1)
        tr_s.name = 'tr'
        return tr_s

    @staticmethod
    def get_rr_port_s(w_df, rr_daily_df, tr_s, method, RA, RE, TC=0.01, offset=0):
        rr_port_df = w_df * rr_daily_df
        rr_port_s = rr_port_df.sum(axis=1) - tr_s * TC
        rr_port_s.name = f"{method}, RA:{RA}, RE:{RE}, TC:{TC}, os:{offset}"
        return rr_port_s

class Backtester_CMC(Backtester):
    def __init__(self, method='a', RA=1, RE=1, TC=0.01, v_filter_period=14, start_date=pd.to_datetime("2020-09-11"), offset=0, isDOWN=False):
        self.method = method
        self.RA = RA
        self.RE = RE
        self.TC = TC
        self.v_filter_period = v_filter_period
        self.start_date = start_date
        self.offset = offset
        self.isDOWN = isDOWN
        self.cumrr_df = pd.DataFrame()
        # self.filter_list = ['c_nan_filter', 'v_filter', 'rr_filter', ''] # 이 순서를 지키도록!

        self.ds = DataScraper(isFromFile=True, isDOWN=isDOWN)
        self.cmcs = CMCScraper()

        self.ohlcv_dic = self.get_ohlcv_dic(mc_lower_limit=0, mc_upper_limit=500)

        self.c_df, self.v_df, self.c_nan_filter = self.get_dfs(self.ohlcv_dic, self.start_date)
        self.v_filter = self.get_v_filter(v_df=self.v_df, v_filter_period=self.v_filter_period)
        self.rr_daily_df = self.get_rr_df(c_df=self.c_df, period=1)

        self.set_strategy(method, RA, RE, TC, offset)

    def get_ohlcv_dic(self, mc_lower_limit, mc_upper_limit):
        b_symbols=[]
        for i in range(len(self.ds.usdt_symbols)):
            b_symbols.append(self.ds.usdt_symbols[i].split('/')[0])
        cmc_symbols = self.cmcs.all_coin_symbol_list[mc_lower_limit:mc_upper_limit]
        symbols = list(set(b_symbols) & set(cmc_symbols))
        print(f"시총 {mc_lower_limit}위부터 {mc_upper_limit}위안에 드는 심볼은 총 {len(symbols)}개 입니다.")
        print(symbols)
        ohlcv_dic = {}
        for symbol in symbols:
            ohlcv_dic[symbol + '/USDT'] = self.ds.usdt_ohlcv_dic[symbol + '/USDT']
        return ohlcv_dic

class Backtester_wos(Backtester_CMC):
    def __init__(self, method='c_25%', RA=1, RE=1, TC=0.01, v_filter_period=14, start_date=pd.to_datetime("2020-09-11"), isDOWN=False): # 25%는 고정! 하드코딩!
        self.method = method
        self.RA = RA
        self.RE = RE
        self.TC = TC
        self.v_filter_period = v_filter_period
        self.start_date = start_date
        self.offset = 0
        self.isDOWN = isDOWN
        self.port_cumrr_df = pd.DataFrame() # offset 통합용 모음
        self.cumrr_df = pd.DataFrame() # 비교용 cumrr_모음

        self.ds = DataScraper(isFromFile=True, isDOWN=isDOWN)
        self.cmcs = CMCScraper()

        self.ohlcv_dic = self.get_ohlcv_dic(mc_lower_limit=0, mc_upper_limit=500)

        self.c_df, self.v_df, self.c_nan_filter = self.get_dfs(self.ohlcv_dic, self.start_date)
        self.v_filter = self.get_v_filter(v_df=self.v_df, v_filter_period=self.v_filter_period)
        self.rr_daily_df = self.get_rr_df(c_df=self.c_df, period=1)

        self.set_strategy(method, RA, RE, TC)

    def set_strategy(self, method, RA, RE, TC):
        self.port_cumrr_df = pd.DataFrame()
        self.rr_filter = self.get_rr_filter(c_df=self.c_df, RA=RA, method=method)
        self.total_filter = self.get_total_filter(c_nan_filter=self.c_nan_filter, v_filter=self.v_filter, rr_filter=self.rr_filter)
        for offset in range(RE):
            self.w_df = self.get_weight_df(rr_daily_df=self.rr_daily_df, filter=self.total_filter, RE=RE, offset=offset)
            self.tr_s = self.get_tr_s(w_df=self.w_df, RE=RE, offset=offset)
            self.rr_port_s = self.get_rr_port_s(w_df=self.w_df, rr_daily_df=self.rr_daily_df, tr_s=self.tr_s, method=method, RA=RA, RE=RE, TC=TC, offset=offset)
            self.cumrr_s = self.plot_crr(rr_port_s=self.rr_port_s)
            self.port_cumrr_df = pd.concat([self.port_cumrr_df, self.cumrr_s], axis=1, join='outer')
        self.cumrr_s = self.port_cumrr_df.sum(axis=1) / RE
        MDD = self.get_MDD(cumrr_s=self.cumrr_s)
        self.cumrr_s.name = f"{method}, RA:{RA}, RE:{RE}, TC:{TC}, MDD:{MDD}"
        pd.DataFrame(self.cumrr_s).plot()

    def plot_crr(self, rr_port_s):
        rr = rr_port_s + 1
        cumrr_s = rr.cumprod()
        # MDD = self.get_MDD(cumrr_s=cumrr_s)
        # cumrr_s.name = f"{rr.name}, MDD:{MDD}"
        # pd.DataFrame(cumrr_s).plot()
        return cumrr_s

def compare_strategy(choice, method, RA, RE, TC, v_filter_period=14, start_date=pd.to_datetime("2020-09-11"), offset=0, iswos=True):
    if not iswos: # iswos = True
        if choice == 'method':
            for i, m in enumerate(('c_25%', 'c_50%', 'c_75%', 'm_25%', 'm_50%', 'm_75%', 'a')):
                if i == 0:
                    bt = Backtester_CMC(method=m, RA=RA, RE=RE, TC=TC, v_filter_period=v_filter_period,
                                    start_date=start_date, offset=offset)
                    bt.add_to_crr_df(bt.cumrr_s)
                else:
                    bt.set_strategy(method=m, RA=RA, RE=RE, TC=TC, offset=offset)
                    bt.add_to_crr_df(bt.cumrr_s)
        elif choice == 'RA':
            for i, ra in enumerate((7, 14, 20, 30)):
                if i == 0:
                    bt = Backtester_CMC(method=method, RA=ra, RE=RE, TC=TC, v_filter_period=v_filter_period,
                                    start_date=start_date, offset=offset)
                    bt.add_to_crr_df(bt.cumrr_s)
                else:
                    bt.set_strategy(method=method, RA=ra, RE=RE, TC=TC, offset=offset)
                    bt.add_to_crr_df(bt.cumrr_s)
        elif choice == 'RE':
            for i, re in enumerate((14, 20, 30, 60)):
                if i == 0:
                    bt = Backtester_CMC(method=method, RA=RA, RE=re, TC=TC, v_filter_period=v_filter_period,
                                    start_date=start_date, offset=offset)
                    bt.add_to_crr_df(bt.cumrr_s)
                else:
                    bt.set_strategy(method=method, RA=RA, RE=re, TC=TC, offset=offset)
                    bt.add_to_crr_df(bt.cumrr_s)
        elif choice == 'TC':
            for i, tc in enumerate((0.005, 0.01, 0.02)):
                if i == 0:
                    bt = Backtester_CMC(method=method, RA=RA, RE=RE, TC=tc, v_filter_period=v_filter_period,
                                    start_date=start_date, offset=offset)
                    bt.add_to_crr_df(bt.cumrr_s)
                else:
                    bt.set_strategy(method=method, RA=RA, RE=RE, TC=tc, offset=offset)
                    bt.add_to_crr_df(bt.cumrr_s)
        elif choice == 'offset':
            for i, os in enumerate(range(RE)):
                if i == 0:
                    bt = Backtester_CMC(method=method, RA=RA, RE=RE, TC=TC, v_filter_period=v_filter_period,
                                    start_date=start_date, offset=os)
                    bt.add_to_crr_df(bt.cumrr_s)
                else:
                    bt.set_strategy(method=method, RA=RA, RE=RE, TC=TC, offset=os)
                    bt.add_to_crr_df(bt.cumrr_s)
        elif choice == 'total':
            # i = 0
            for m in ('c_25%', 'c_50%', 'c_75%', 'm_25%', 'm_50%', 'm_75%', 'a'):
                for i, ra in enumerate((3, 7, 14, 30)):
                    for re in (3, 7, 14, 30):
                        tc = 0.01
                        if i == 0:
                            bt = Backtester_CMC(method=m, RA=ra, RE=re, TC=tc, v_filter_period=v_filter_period,
                                            start_date=start_date, offset=offset)
                            bt.add_to_crr_df(bt.cumrr_s)
                            i += 1
                        else:
                            bt.set_strategy(method=m, RA=ra, RE=re, TC=tc, offset=offset)
                            bt.add_to_crr_df(bt.cumrr_s)

            for i, os in enumerate(range(RE)):
                if i == 0:
                    bt = Backtester_CMC(method=method, RA=RA, RE=RE, TC=TC, v_filter_period=v_filter_period,
                                    start_date=start_date, offset=os)
                    bt.add_to_crr_df(bt.cumrr_s)
                else:
                    bt.set_strategy(method=method, RA=RA, RE=RE, TC=TC, offset=os)
                    bt.add_to_crr_df(bt.cumrr_s)
    else: # iswos = True
        if choice == 'method':
            for i, m in enumerate(('c_25%', 'c_50%', 'c_75%', 'm_25%', 'm_50%', 'm_75%', 'a')):
                if i == 0:
                    bt = Backtester_wos(method=m, RA=RA, RE=RE, TC=TC, v_filter_period=v_filter_period,
                                    start_date=start_date)
                    bt.add_to_crr_df(bt.cumrr_s)
                else:
                    bt.set_strategy(method=m, RA=RA, RE=RE, TC=TC)
                    bt.add_to_crr_df(bt.cumrr_s)
        elif choice == 'RA':
            for i, ra in enumerate((7, 14, 20, 30)):
                if i == 0:
                    bt = Backtester_wos(method=method, RA=ra, RE=RE, TC=TC, v_filter_period=v_filter_period,
                                    start_date=start_date)
                    bt.add_to_crr_df(bt.cumrr_s)
                else:
                    bt.set_strategy(method=method, RA=ra, RE=RE, TC=TC)
                    bt.add_to_crr_df(bt.cumrr_s)
        elif choice == 'RE':
            for i, re in enumerate((7, 14, 20, 30)):
                if i == 0:
                    bt = Backtester_wos(method=method, RA=RA, RE=re, TC=TC, v_filter_period=v_filter_period,
                                    start_date=start_date)
                    bt.add_to_crr_df(bt.cumrr_s)
                else:
                    bt.set_strategy(method=method, RA=RA, RE=re, TC=TC)
                    bt.add_to_crr_df(bt.cumrr_s)
        elif choice == 'TC':
            for i, tc in enumerate((0.005, 0.01, 0.02)):
                if i == 0:
                    bt = Backtester_wos(method=method, RA=RA, RE=RE, TC=tc, v_filter_period=v_filter_period,
                                    start_date=start_date)
                    bt.add_to_crr_df(bt.cumrr_s)
                else:
                    bt.set_strategy(method=method, RA=RA, RE=RE, TC=tc)
                    bt.add_to_crr_df(bt.cumrr_s)
        elif choice == 'offset':
            for i, os in enumerate(range(1, RE)):
                if i == 0:
                    bt = Backtester_CMC(method=method, RA=RA, RE=RE, TC=TC, v_filter_period=v_filter_period,
                                    start_date=start_date, offset=os)
                    bt.add_to_crr_df(bt.cumrr_s)
                else:
                    bt.set_strategy(method=method, RA=RA, RE=RE, TC=TC, offset=os)
                    bt.add_to_crr_df(bt.cumrr_s)
        elif choice == 'total':
            # i = 0
            for m in ('m_25%', 'm_50%', 'm_75%', 'a'):
                for i, ra in enumerate((7, 14, 20, 30)):
                    for re in (14, 20, 30):
                        tc = 0.01
                        if i == 0:
                            bt = Backtester_wos(method=m, RA=ra, RE=re, TC=tc, v_filter_period=v_filter_period,
                                                start_date=start_date)
                            bt.add_to_crr_df(bt.cumrr_s)
                            i += 1
                        else:
                            bt.set_strategy(method=m, RA=ra, RE=re, TC=tc)
                            bt.add_to_crr_df(bt.cumrr_s)

            for i, os in enumerate(range(1, RE)):
                if i == 0:
                    bt = Backtester_CMC(method=method, RA=RA, RE=RE, TC=TC, v_filter_period=v_filter_period,
                                    start_date=start_date, offset=os)
                    bt.add_to_crr_df(bt.cumrr_s)
                else:
                    bt.set_strategy(method=method, RA=RA, RE=RE, TC=TC, offset=os)
                    bt.add_to_crr_df(bt.cumrr_s)


if __name__ == "__main__":

    bt = Backtester(method='c_25%', RA=1, RE=7, TC=0.01, v_filter_period=14, start_date=pd.to_datetime("2020-09-11"), offset=0, isDOWN=False)
    # ohlcv_dic = bt.ohlcv_dic
    # c_df = bt.c_df
    # v_df = bt.v_df
    # c_nan_filter = bt.c_nan_filter
    # v_filter = bt.v_filter
    # rr_filter = bt.rr_filter
    # rr_daily_df = bt.rr_daily_df
    # total_filter = bt.total_filter
    # w_df = bt.w_df
    # tr_s = bt.tr_s
    # rr_port_s = bt.rr_port_s
    print('done')
