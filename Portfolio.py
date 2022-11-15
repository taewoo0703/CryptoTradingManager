import datetime as dt

def ff(time):
    print(type(time))

class Portfolio:
    def __init__(self, volume, start_date, end_date, RA='1w', RE='1d', TC=0.01, N=0.25, v_filter=100):
        self.coin_list = []
        self.RA = self.set_RA_RE(RA) # Ranking Window
        self.RE = self.set_RA_RE(RE) # Rebalancing Period
        self.TC = TC # Transaction Cost = 1%
        self.N = N # %N = 25%
        self.v_filter = 100 # 14일 거래량 이동평균 필터 = 100 USD

        self.start_date = start_date
        self.end_date = end_date
        self.current_date = start_date

    def set_RA_RE(self, period):
        value = int(period[:-1])
        unit = period[-1]
        if unit == 'd':
            return dt.timedelta(days=value)
        if unit == 'w':
            return dt.timedelta(weeks=value)

