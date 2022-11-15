import ccxt
import pandas as pd
import os
from DataScraper import *
from Backtester import *
import time
import pickle

class Trader(DataScraper):
    def __init__(self, method, RA, RE, v_filter_period=7, additional_usdt=0, divide=1, Binance_API_KEY=Binance_API_KEY, Binance_SECRET_KEY=Binance_SECRET_KEY):
        """
        :param method: "m_25%" : 모멘텀 하위 25%를 제외한 종목 매수
        :param RA: 가격 변화율 관찰 기간 (RA = 7 이면 7일간의 가격 변화율 관찰)
        :param RE: 리밸런싱 기간
        :param v_filter_period: 거래량 필터 기간
        :param additional_usdt: 추가할 돈
        :param divide: 포트폴리오 분할
        :param Binance_API_KEY:
        :param Binance_SECRET_KEY:
        """
        # region DataScraper __init__
        self.binance = ccxt.binance(config={
            'apiKey': Binance_API_KEY,
            'secret': Binance_SECRET_KEY
        })
        self.all_markets = {}
        self.usdt_markets = {}
        self.usdt_symbols = []
        self.usdt_tickers = {}
        self.usdt_ohlcv_dic = {}
        self.busd_markets = {}
        self.busd_symbols = []
        self.busd_tickers = {}
        self.busd_ohlcv_dic = {}
        self.file_path = "./ohlcv/"
        self.file_list = []
        self.initialize_common()
        self.usdt_ohlcv_dic = self.get_OHLCV(self.usdt_symbols)  # key : 'BTC/USDT' , value : OHLCV
        self.busd_ohlcv_dic = self.get_OHLCV(self.busd_symbols)
        print("DataScript init done")
        # endregion

        # region 전략 파라미터 설정 (From Backtester)
        self.method = method
        self.RA = RA
        self.RE = RE
        self.start_date = self.usdt_ohlcv_dic['BTC/USDT']['datetime'].iloc[0]
        self.v_filter_period = v_filter_period
        # endregion

        # region 전략에 따른 보유할 종목 coin_list 가져오기
        self.cmcs = CMCScraper()
        self.ohlcv_dic = self.get_ohlcv_dic(mc_lower_limit=0, mc_upper_limit=500) # USDT ohlcv를 시총 필터링 먹인 것.
        self.c_df, self.v_df, self.c_nan_filter = Backtester.get_dfs(ohlcv_dic=self.ohlcv_dic, start_date=self.start_date) # 스테이블 코인 제외
        self.v_filter = Backtester.get_v_filter(v_df=self.v_df, v_filter_period=self.v_filter_period)
        self.rr_daily_df = Backtester.get_rr_df(c_df=self.c_df, period=1)

        self.rr_filter = Backtester.get_rr_filter(c_df=self.c_df, RA=RA, method=method)
        self.total_filter = self.c_nan_filter & self.v_filter & self.rr_filter
        self.coin_list = self.get_coin_list(filter=self.total_filter)
        self.w_s = self.get_weight_s(coin_list=self.coin_list, c_df=self.c_df)
        print("전략 계산하여 종목가져오기 done")
        # endregion

        # region Trading 변수 선언
        self.history_path = './history/'
        self.bal = self.binance.fetch_balance()
        self.std_date = pd.to_datetime("2022-01-01")
        self.offset = self.get_offset(std_date=self.std_date, RE=RE) # 0 <= offset < RE
        # endregion

        # region Trading
        self.usdt = self.get_usdt_rebal(add_usdt=additional_usdt, divide=divide)
        # endregion

    def get_OHLCV(self, symbols): # 33일치 데이터 조회
        """
        :param symbols: ['BTC/USDT', 'ETH/USDT', ... ]
        :param isSave: csv파일을 저장할거면 True
        :return: dictionary ( key : symbol, value : OHLCV 데이터)
        """
        if self.binance.has['fetchOHLCV']:
            ohlcv_dic = {}
            for symbol in symbols:
                time.sleep(1.5 * self.binance.rateLimit / 1000)  # time.sleep wants seconds
                ohlcv_list = self.binance.fetch_ohlcv(symbol, '1d', limit=33)  # one day
                ohlcv_df = self.ohlcv_list_to_df(ohlcv_list)
                ohlcv_dic[symbol] = ohlcv_df
                print(symbol, "OHLCV get!")
            return ohlcv_dic
        else:
            print("get_OHLCV method failed!")
            return {}

    def get_ohlcv_dic(self, mc_lower_limit=0, mc_upper_limit=500):
        b_symbols=[]
        for i in range(len(self.usdt_symbols)):
            b_symbols.append(self.usdt_symbols[i].split('/')[0])
        cmc_symbols = self.cmcs.all_coin_symbol_list[mc_lower_limit:mc_upper_limit]
        symbols = list(set(b_symbols) & set(cmc_symbols))
        print(f"시총 {mc_lower_limit}위부터 {mc_upper_limit}위안에 드는 심볼은 총 {len(symbols)}개 입니다.")
        print(symbols)
        ohlcv_dic = {}
        for symbol in symbols:
            ohlcv_dic[symbol + '/USDT'] = self.usdt_ohlcv_dic[symbol + '/USDT']
        return ohlcv_dic

    @staticmethod
    def get_coin_list(filter):
        t = time.localtime()
        if t.tm_hour <= 9: # 오전 9시 이전 : 당일 매수 시그널 사용
            f = filter.iloc[-1]
        else: # 오전 9시 이후 : 1일전 매수 시그널 사용
            f = filter.iloc[-2]
        coin_list = list(f.loc[f].index)
        return coin_list

    def get_weight_s(self, coin_list, c_df): # 임시로 동일 배분으로 함.
        w_s = pd.Series(data=0, index=coin_list) # 초기화
        weight = 1 / len(coin_list) # 동일 배분 가중치
        w_s += weight
        return w_s

    def get_usdt_rebal(self, add_usdt, divide):
        balance = self.bal['USDT']['free']
        if 1 <= divide <= self.RE:
            return min(balance, add_usdt / divide)
        else: # 최대분할
            return min(balance, add_usdt / self.RE)

    def rebalancing(self):
        self.ex_bal = self.get_ex_bal(offset=self.offset, history_path=self.history_path)
        print('get_ex_bal done')
        self.fresh_tickers = self.get_tickers(set(self.coin_list) | set(self.ex_bal.index))
        print('get_fresh_tickers done')
        if self.ex_bal.empty:
            self.value = self.usdt
        else:
            self.ex_price, self.ex_value = self.get_ex_value(ex_bal=self.ex_bal, tickers=self.fresh_tickers)
            self.value = self.ex_value.sum() + self.usdt  # 포트폴리오 현재 환산가치
            print('get_ex_value done')
        self.port_price, self.port_bal = self.get_port_bal(value=self.value, w_s=self.w_s)
        print("get_port_bal done")
        self.rebal_s, self.port_bal = self.get_rebal_s(ex_bal=self.ex_bal, port_bal=self.port_bal, tickers=self.fresh_tickers)
        print("get_rebal_s done")
        print(self.rebal_s)
        self.order_id_dic, self.error_symbol_list = self.create_order(rebal_s=self.rebal_s, tickers=self.fresh_tickers)
        print("create_order done")
        print(f"error_symbol list length = {len(self.error_symbol_list)}")
        print(f"error_symbol : {self.error_symbol_list}")
        print(f"추후 csv 직접 수정 필요")


    def get_offset(self, std_date, RE): # offset 은 0부터 RE-1 중에 하나
        now = time.localtime()
        today = pd.to_datetime(f"{now.tm_year}-{now.tm_mon}-{now.tm_mday}")
        offset = (today - std_date).days % RE
        return offset

    def get_ex_bal(self, offset=0, history_path='./history/'):
        file_list = os.listdir(history_path)
        file_name = f"history_offset_{offset}.csv"
        if file_name in file_list: # 파일 있는 경우
            file_df = pd.read_csv(history_path + file_name)
            file_df['datetime'] = pd.to_datetime(file_df['datetime'])
            file_df = file_df.set_index('datetime')
            ex_bal = file_df.iloc[-1]
            ex_bal.name = 'ex_bal'
        else: # 파일 없는 경우
            ex_bal = pd.Series()

        return ex_bal

    def get_ex_value(self, ex_bal, tickers):
        if not ex_bal.empty: # 과거 매수기록이 있는 경우
            ex_price = pd.Series(data=0, index=ex_bal.index, name='ex_price')
            for symbol in ex_bal.index:
                ex_price.loc[symbol] = tickers[symbol]['last']
            ex_value = ex_price * ex_bal
            ex_value.name = 'ex_value'
            return ex_price, ex_value
        else: # 과거 매수기록이 없는 경우
            return pd.Series(), pd.Series()

    def get_port_bal(self, value, w_s):
        port_value = w_s * value
        port_price = pd.Series(data=0, index=w_s.index, name='port_price')
        for symbol in w_s.index:
            port_price.loc[symbol] = self.fresh_tickers[symbol]['last']
        port_bal = port_value / port_price
        port_bal.name = 'port_bal'
        return port_price, port_bal

    def check_10USD_each(self, value_s):
        check = (value_s <= 10)
        print(f"10달러 이하 포지션이 {len(value_s)}개 중에 {check.sum()} 개 있습니다.")

    def get_rebal_s(self, ex_bal, port_bal, tickers):
        # ex_bal과 port_bal 기준으로 rebal_s 도출 ( rebal_s = port_bal - ex_bal )
        if not ex_bal.empty: # 과거 매수기록이 있는 경우
            rebal_df = pd.concat([ex_bal, port_bal], axis=1, join='outer')
            self.rebal_df = rebal_df.fillna(value=0)
            rebal_s = self.rebal_df['port_bal'] - self.rebal_df['ex_bal'] # 매수할건 (+) 매도할건 (-)로 표현
            rebal_s.name = 'rebal_s'
        else: # 과거 매수기록이 없는 경우
            rebal_s = port_bal.copy()
            rebal_s.name = 'rebal_s'

        # rebal_s에서 밸류가 10USD 이하인 것은 0으로 없앰
        price = pd.Series(data=0, index=rebal_s.index, name='price')
        for symbol in rebal_s.index:
            price.loc[symbol] = tickers[symbol]['last']
        rebal_value = price * abs(rebal_s)
        self.check_10USD_each(value_s=rebal_value)
        rebal_s.loc[rebal_value <= 10] = 0

        # 변경한 rebal_s 기준으로 port_bal 재조정 ( port_bal = ex_bal + rebal_s )
        if not ex_bal.empty: # 과거 매수기록이 있는 경우
            new_port_bal = ex_bal + rebal_s
        else:  # 과거 매수기록이 없는 경우
            new_port_bal = rebal_s

        return rebal_s, new_port_bal

    def create_order(self, rebal_s, tickers):
        order_id_dic = {}
        error_symbol_list = []
        for symbol in rebal_s.index:
            amount = rebal_s.loc[symbol]
            time.sleep(2 * self.binance.rateLimit / 1000)  # time.sleep wants seconds
            if amount > 0: # 매수
                try:
                    order = self.binance.create_limit_buy_order(symbol=symbol, amount=amount, price=tickers[symbol]['last'])
                    order_id_dic[symbol] = order['info']['orderId']
                except:
                    print(f"{symbol} makes some [buy_order_error]")
                    error_symbol_list.append(symbol)
            else: # 매도
                try:
                    order = self.binance.create_limit_sell_order(symbol=symbol, amount=amount, price=tickers[symbol]['last'])
                    order_id_dic[symbol] = order['info']['orderId']
                except:
                    print(f"{symbol} makes some [sell_order_error]")
                    error_symbol_list.append(symbol)

        return order_id_dic, error_symbol_list

    # def write_ex_bal_csv(self, path, port_bal, offset):
    #     now = time.localtime()
    #     today = pd.to_datetime(f"{now.tm_year}-{now.tm_mon}-{now.tm_mday}")
    #     port_bal.name = today
    #     file_list = os.listdir(path)
    #     file_name = f"history_offset_{offset}.csv"
    #     if file_name in file_list: # update file
    #         file_df = pd.read_csv(path + file_name)
    #         file_df['datetime'] = pd.to_datetime(file_df['datetime'])
    #         file_df = file_df.set_index('datetime')
    #         file_df = file_df.append(port_bal)
    #         file_df.to_csv(path + file_name, mode='w')
    #     else: # 새 파일 생성
    #         file_df = pd.DataFrame(port_bal).T
    #         file_df.index.name = 'datetime'
    #         file_df.to_csv(path + file_name, mode='w')


    def sell_all_coins(self):
        bal = self.binance.fetch_balance()
        tickers = self.binance.fetch_tickers()
        coin_list = []
        for sym in bal.keys():
            try:
                amount = bal[sym]['free']
                price = tickers[f"{sym}/USDT"]["last"]
                if amount * price > 10:
                    coin_list.append(f"{sym}/USDT")
            except:
                print("?")
        print(coin_list)
        for sym in coin_list:
            try:
                self.binance.create_market_sell_order(symbol=sym, amount=bal[sym.split('/')[0]]['free'])
                # print("sell complete : " + sym)

            except:
                print("sell failed : " + sym)




    def write_ex_bal_csv(self, path, rebal_s, offset, old_bal):
        new_bal = self.binance.fetch_balance()

        now = time.localtime()
        today = pd.to_datetime(f"{now.tm_year}-{now.tm_mon}-{now.tm_mday}")
        bal_change = pd.Series(data=0, index=rebal_s.index, name='bal_change')
        for symbol in rebal_s.index:
            bal_change.loc[symbol] = new_bal[symbol.split('/')[0]]['free'] - old_bal[symbol.split('/')[0]]['free']

        file_list = os.listdir(path)
        file_name = f"history_offset_{offset}.csv"
        if file_name in file_list: # update file
            file_df = pd.read_csv(path + file_name)
            file_df['datetime'] = pd.to_datetime(file_df['datetime'])
            file_df = file_df.set_index('datetime')
            # old_port_bal + bal_change = new_port_bal
            old_port_bal = file_df.iloc[-1]
            tmp_df = pd.concat([old_port_bal, bal_change], axis=1, join='outer')
            tmp_df = tmp_df.fillna(value=0)
            new_port_bal = tmp_df[old_port_bal.name] + tmp_df[bal_change.name]
            new_port_bal.name = today
            # 최하단 row에 new_port_bal 추가한 뒤 저장
            file_df = file_df.append(new_port_bal)
            file_df.to_csv(path + file_name, mode='w')
        else: # 새 파일 생성
            bal_change.name = today
            file_df = pd.DataFrame(bal_change).T
            file_df.index.name = 'datetime'
            file_df.to_csv(path + file_name, mode='w')
        return bal_change

if __name__ == "__main__":
    t = Trader(method='m_25%', RA=30, RE=30, v_filter_period=7, additional_usdt=3000, divide=1, Binance_API_KEY=Binance_API_KEY, Binance_SECRET_KEY=Binance_SECRET_KEY)
    t.rebalancing()
    # t.sell_all_coins()
    # time.sleep(3600) # sleep 1hour
    t.write_ex_bal_csv(path=t.history_path, rebal_s=t.rebal_s, offset=t.offset, old_bal=t.bal)
    print(f"offset : {t.offset}, 저장완료")
    print('done')