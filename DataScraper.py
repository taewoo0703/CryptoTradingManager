import ccxt
import time
import pandas as pd
import os
from requests import Request, Session
from requests.exceptions import ConnectionError, Timeout, TooManyRedirects
import json

# region API KEY
Binance_API_KEY = "90IGrd6MtBRv1ZPOgAWjQv8Zo8grIBTPMTkDQ8bWYkUW3NDFNQ8gcdaouDGKjCFr"
Binance_SECRET_KEY = "T37EykRUnTJWl7AUcBGq15WpBQ9dclvTqE55xsgtRh4GecrUFBxeqV4ZAtpvQZGl"
CMC_API_KEY = "5e9def60-55b2-4ed0-b0da-31b8c9a9bd4f"
CMC_API_KEY_GMAIL = "8ad5ede9-75ab-400a-a082-ef9c6a12d606"
# endregion

class DataScraper:
    def __init__(self, isFromFile=False, isDOWN=False):
        # region API KEY
        self.Binance_API_KEY = "90IGrd6MtBRv1ZPOgAWjQv8Zo8grIBTPMTkDQ8bWYkUW3NDFNQ8gcdaouDGKjCFr"
        self.Binance_SECRET_KEY = "T37EykRUnTJWl7AUcBGq15WpBQ9dclvTqE55xsgtRh4GecrUFBxeqV4ZAtpvQZGl"
        # endregion
        self.binance = ccxt.binance()
        # region 변수 초기화
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
        # endregion
        if not isDOWN:
            if isFromFile: # OHLCV를 파일로부터 불러오는 경우
                self.initialize_common()
                self.usdt_ohlcv_dic = self.load_OHLCV(self.usdt_symbols)  # key : 'BTC/USDT' , value : OHLCV
                self.busd_ohlcv_dic = self.load_OHLCV(self.busd_symbols)  # key : 'BTC/BUSD' , value : OHLCV

            else: # OHLCV를 ccxt로부터 가져오는 경우
                self.initialize_common()
                self.usdt_ohlcv_dic = self.get_OHLCV(self.usdt_symbols)  # key : 'BTC/USDT' , value : OHLCV
                self.busd_ohlcv_dic = self.get_OHLCV(self.busd_symbols)  # key : 'BTC/BUSD' , value : OHLCV
        if isDOWN:
            self.file_path = "./ohlcv_down/"
            if isFromFile:  # OHLCV를 파일로부터 불러오는 경우
                self.all_markets = self.binance.load_markets()
                self.usdt_markets = self.get_fiat_markets(self.all_markets, fiat="USDT")
                self.usdt_symbols = list(self.usdt_markets.keys())
                self.usdt_symbols = self.get_symbols_DOWN(self.usdt_symbols)
                self.usdt_tickers = self.get_tickers(self.usdt_symbols)
                self.usdt_ohlcv_dic = self.load_OHLCV(self.usdt_symbols)  # key : 'BTC/USDT' , value : OHLCV

            else:  # OHLCV를 ccxt로부터 가져오는 경우
                self.all_markets = self.binance.load_markets()
                self.usdt_markets = self.get_fiat_markets(self.all_markets, fiat="USDT")
                self.usdt_symbols = list(self.usdt_markets.keys())
                self.usdt_symbols = self.get_symbols_DOWN(self.usdt_symbols)
                self.usdt_tickers = self.get_tickers(self.usdt_symbols)
                self.usdt_ohlcv_dic = self.get_OHLCV(self.usdt_symbols)  # key : 'BTC/USDT' , value : OHLCV

    def initialize_common(self):
        self.all_markets = self.binance.load_markets()

        self.usdt_markets = self.get_fiat_markets(self.all_markets, fiat="USDT")
        self.usdt_symbols = list(self.usdt_markets.keys())
        self.usdt_symbols = self.remove_symbols_UP_DOWN_BULL_BEAR(self.usdt_symbols)
        self.usdt_tickers = self.get_tickers(self.usdt_symbols)

        self.busd_markets = self.get_fiat_markets(self.all_markets, fiat="BUSD")
        self.busd_symbols = list(self.busd_markets.keys())
        self.busd_symbols = self.remove_symbols_UP_DOWN_BULL_BEAR(self.busd_symbols)
        self.busd_tickers = self.get_tickers(self.busd_symbols)

    def remove_symbols_UP_DOWN_BULL_BEAR(self, symbols):
        new_symbols = []
        for symbol in symbols:
            if symbol.split('/')[0].endswith('UP') or symbol.split('/')[0].endswith('DOWN') or \
                    symbol.split('/')[0].endswith('BULL') or symbol.split('/')[0].endswith('BEAR'):
                pass
            else:
                new_symbols.append(symbol)
        return new_symbols

    def get_symbols_DOWN(self, symbols):
        new_symbols = []
        for symbol in symbols:
            if  symbol.split('/')[0].endswith('DOWN'):
                new_symbols.append(symbol)
        return new_symbols

    def get_fiat_markets(self, all_markets, fiat="USDT"):
        """
        :param all_markets: 모든 화폐 pair에 대한 market dictionary
        :param fiat: 'USDT', 'BUSD' 등과 같은 스테이블 코인 symbol
        :return: 해당 fiat을 기반으로 하는 모든 화폐 pair에 대한 market dictionary
        """
        fiat_markets = {}
        for key in all_markets.keys():
            if key.split('/')[1] == fiat:
                fiat_markets[key] = all_markets[key]
        return fiat_markets

    def view_order_book(self, symbol):
        """
        :param symbol: 'BTC/USDT' 같은 형태
        :return: void
        """
        orderbook = self.binance.fetch_order_book(symbol=symbol)
        bid = orderbook['bids'][0][0] if len(orderbook['bids']) > 0 else None
        ask = orderbook['asks'][0][0] if len(orderbook['asks']) > 0 else None
        spread = (ask - bid) if (bid and ask) else None
        print(self.binance.id, 'market price', {'bid': bid, 'ask': ask, 'spread': spread})

    def get_tickers(self, symbols):
        """
        :param symbols: ['BTC/USDT', 'ETH/USDT', ... ]
        :return: symbol에 대한 현재가격 및 호가, 거래량 등 정보
        """
        if self.binance.has['fetchTickers']:
            return self.binance.fetch_tickers(symbols=symbols)
        else:
            print("get_tickers method failed!")
            return {}

    def get_OHLCV(self, symbols):
        """
        :param symbols: ['BTC/USDT', 'ETH/USDT', ... ]
        :param isSave: csv파일을 저장할거면 True
        :return: dictionary ( key : symbol, value : OHLCV 데이터)
        """
        if self.binance.has['fetchOHLCV']:
            ohlcv_dic = {}
            for symbol in symbols:
                time.sleep(1.5 * self.binance.rateLimit / 1000)  # time.sleep wants seconds
                ohlcv_list = self.binance.fetch_ohlcv(symbol, '1d')  # one day
                ohlcv_df = self.ohlcv_list_to_df(ohlcv_list)
                ohlcv_dic[symbol] = ohlcv_df
                print(symbol, "OHLCV get!")
            return ohlcv_dic
        else:
            print("get_OHLCV method failed!")
            return {}

    @staticmethod
    def ohlcv_list_to_df(ohlcv_list):
        """
        :param ohlcv_list: ccxt를 사용해 얻은 OHLCV 리스트
        :return: OHLCV를 담은 DataFrame
        """
        ohlcv_df = pd.DataFrame(ohlcv_list, columns=['datetime', 'open', 'high', 'low', 'close', 'volume'])
        ohlcv_df['datetime'] = pd.to_datetime(ohlcv_df['datetime'], unit='ms')
        # ohlcv_df.set_index('datetime', inplace=True)
        return ohlcv_df

    def update_all_OHLCV_csv(self):
        """
        usdt랑 busd랑 둘다 OHLCV 저장
        :return: void
        """
        self.update_OHLCV_csvs(self.usdt_ohlcv_dic)
        self.update_OHLCV_csvs(self.busd_ohlcv_dic)

    def update_OHLCV_csvs(self, ohlcv_dic):
        fiat = list(ohlcv_dic.keys())[0].split('/')[1] # USDT
        self.file_list = os.listdir(self.file_path + fiat)
        for key in ohlcv_dic.keys():  # key = 'BTC/USDT'
            filename = f"{key.split('/')[0]}_{key.split('/')[1]}.csv" # filename = 'BTC_USDT.csv"
            self.write_OHLCV_csv(dataframe=ohlcv_dic[key], filename=filename, filepath=self.file_path+key.split('/')[1]+'/')

    def write_OHLCV_csv(self, dataframe, filename, filepath):
        """
        역할 : 기존의 파일 체크 후 업데이트 혹은 새로운 파일 생성
        :param dataframe: 저장할 df
        :param filename: 저장할 파일 이름
        :param filepath: 파일 경로
        :return: void
        """
        if filename in self.file_list: # update file
            file_df = pd.read_csv(filepath+filename)
            file_df['datetime'] = pd.to_datetime(file_df['datetime'])
            file_latest_date = file_df['datetime'].iloc[-1]
            df_update = dataframe.loc[dataframe['datetime'] > file_latest_date] # 최신 df만 선별.

            if len(df_update.index) != 0: # "not df_update.empty"와 동일한 의미
                df_update.to_csv(filepath+filename, mode='a', index=False, header=False) # 저장

        else: # generate new file
            dataframe.to_csv(filepath+filename, mode='w', index=False)

    def load_OHLCV(self, symbols):
        ohlcv_dic = {}
        fiat = symbols[0].split('/')[1]+'/'
        file_list = os.listdir(self.file_path + fiat)
        # check isUpdated
        symbols_based_file_list = [f"{symbol.split('/')[0]}_{symbol.split('/')[1]}.csv" for symbol in symbols]
        if not (set(file_list) >= set(symbols_based_file_list)):
            print("U need to 'update_all_OHLCV_csv' first. And then Try 'load_OHLCV' again.")
            return {}

        if len(file_list) == 0:
            print(f"There is no file in '{self.file_path + fiat}'")
        for filename in file_list:
            symbol = filename.rstrip(".csv").split('_')[0] + '/' + filename.rstrip(".csv").split('_')[1]
            file_df = self.read_OHLCV_csv(filename=filename, filepath=self.file_path+fiat)
            ohlcv_dic[symbol] = file_df

        # check isUpdated
        if (set(ohlcv_dic.keys()) == set(symbols)):
            print("load_OHLCV complete!")
        else:
            print("load_OHLCV Incomplete!")

        return ohlcv_dic

    def read_OHLCV_csv(self, filename, filepath):
        file_df = pd.read_csv(filepath + filename)
        file_df['datetime'] = pd.to_datetime(file_df['datetime'])
        return file_df

class CMCScraper:
    def __init__(self, CMC_API_KEY="5e9def60-55b2-4ed0-b0da-31b8c9a9bd4f"):
        self.API_KEY = CMC_API_KEY
        self.all_coin_id_list = []
        self.all_coin_symbol_list = []
        self.all_coin_name_list = []
        self.all_coin_slug_list = []
        self.get_all_coin_list(self.API_KEY)

    def get_all_coin_list(self, API_KEY):
        url = 'https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest'
        headers = {
            'Accepts': 'application/json',
            'X-CMC_PRO_API_KEY': API_KEY,
        }
        session = Session()
        session.headers.update(headers)
        try:
            parameters = {'start': '1', 'limit': '5000', 'convert': 'USD'}
            response = session.get(url, params=parameters)
            data = json.loads(response.text)
            for i in range(len(data.get('data'))):
                self.all_coin_id_list.append(str(data.get('data')[i]['id']))
                self.all_coin_symbol_list.append(data.get('data')[i]['symbol'])
                self.all_coin_name_list.append(data.get('data')[i]['name'])
                self.all_coin_slug_list.append(data.get('data')[i]['slug'])

            ii = 1
            while data['status']['total_count'] > 5000 and len(data.get('data')) == 5000:
                parameters = {'start': str(5000 * ii + 1), 'limit': '5000', 'convert': 'USD'}
                response = session.get(url, params=parameters)
                data = json.loads(response.text)
                for i in range(len(data.get('data'))):
                    self.all_coin_id_list.append(str(data.get('data')[i]['id']))
                    self.all_coin_symbol_list.append(data.get('data')[i]['symbol'])
                    self.all_coin_name_list.append(data.get('data')[i]['name'])
                    self.all_coin_slug_list.append(data.get('data')[i]['slug'])
                ii += 1

        except (ConnectionError, Timeout, TooManyRedirects) as e:
            print(e)

if __name__ == "__main__":
    print("DataScraper.py is running...")
    ds = DataScraper(isFromFile=False, isDOWN=False)
    ds.update_all_OHLCV_csv() # OHLCV data 저장!
    print('done.')