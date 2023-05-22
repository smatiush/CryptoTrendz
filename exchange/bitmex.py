import ccxt
from pprint import pprint
import pandas
from config.exchangeConfig import BITMEX_KEY, BITMEX_SECRET
from datetime import datetime, timedelta
#import zmq
import sys
import time
import pandas as pd

class BitmexContainer():
    def __init__(self):
        self.exchangeId = 'bitmex'

        self.exchangeInstance = ccxt.bitmex({
            'apiKey': BITMEX_KEY,
            'secret': BITMEX_SECRET,
            'options': { 'enableRateLimit': True }

        })
        self.exchangeMarkets = self.exchangeInstance.load_markets()
        self.pairsData = self._loadSymbolFromExchange()
        self.pollerData = {}

    def _loadSymbolFromExchange(self):
        pairData = {}
        for pair in list(self.exchangeMarkets.keys()):
            if pair.find(':') != -1:
                if pair[pair.find(':')+1:] == 'USDT':
                    pairData[pair] = self.exchangeMarkets[pair]
                else:
                    continue
            else:
                continue
        return pairData

    def getFunding(self, symbol):
        return self.exchangeInstance.fetch_funding_rate(symbol=symbol)

    def getHistoricalFunding(self, symbols = None):
        '''
        :param symbol:
        :param daysBack: in S
        :return:
        '''
        funding = []
        #daysBack = 2629743
        historicalFunding = pd.DataFrame()
        if symbols == None:
            for symbol in self.pairsData.keys():

                fundingData = self.exchangeInstance.fetch_funding_rate_history(symbol=symbol ,params={
                    'startTime': datetime.now() - timedelta(days=60),
                    'reverse' : True,
                    'limit' : 200
                })#'startTime': datetime.now() - timedelta(days=60) } )
                if len(fundingData) == 0:
                    continue
                else:
                    for fund in fundingData:
                        #pprint(fund)
                        funding.append({'fundingRate': fund['fundingRate'], 'timestamp': fund['timestamp']})
                    #print('-------funding-------')
                    #pprint(funding)
                    temp_df = pd.DataFrame(funding)
                    temp_df.set_index('timestamp', inplace=True)
                temp_df = temp_df.squeeze().rename(symbol)
                historicalFunding = pd.concat([historicalFunding, temp_df], axis=1)
                funding = []
                print('BITMEX: ' + symbol)
        else:
            for symbol in symbols:
                print('BITMEX: ' + symbol)
                symbol = symbol + '/USDT:USDT'
                fundingData = self.exchangeInstance.fetch_funding_rate_history(symbol=symbol, params={
                    'startTime': datetime.now() - timedelta(days=60),
                    'reverse' : True,
                    'limit' : 200
                })
                for fund in fundingData:
                    # pprint(fund)
                    funding.append({'fundingRate': fund['fundingRate'], 'timestamp': fund['timestamp']})
                temp_df = pd.DataFrame(funding)
                temp_df.set_index('timestamp', inplace=True)
                temp_df = temp_df.squeeze().rename(symbol)
                historicalFunding = pd.concat([historicalFunding, temp_df], axis=1)
                funding = []
        return historicalFunding

    def getAccountBalance(self):
        return self.exchangeInstance.fetch_balance()['total']

    def getOB(self, symbol):
        return self.exchangeInstance.fetch_order_book(symbol=symbol, limit=5)

    def getAccountPosition(self, symbol):
        pos = self.exchangeInstance.fetch_positions(symbols=[symbol])
        return pos


if __name__ == '__main__':
    e = BitmexContainer()
    pprint(e.getAccountPosition('BTC/USDT:USDT'))
