import sys

import ccxt
import requests
import pandas as pd
from pprint import pprint
from config.exchangeConfig import BYBIT_KEY, BYBIT_SECRET
from datetime import datetime, timedelta

import time


class BybitContainer():
    def __init__(self):
        self.exchangeId = 'bybit'
        self.exchangeInstance = ccxt.bybit({
            'apiKey': BYBIT_KEY,
            'secret': BYBIT_SECRET,
            'options':
                { 'enableRateLimit': True }
        })
        self.exchangeInstance.options['createMarketBuyOrderRequiresPrice'] = False
        self.exchangeInstance.options['enableUnifiedMargin'] = True
        self.exchangeMarkets = self.exchangeInstance.load_markets()
        self.pairsData = self._loadSymbolFromExchange()
        self.pollerData = {}

    def changeSPOTtoSWAP(self, type):
        if type == 'swap':
            self.exchangeInstance.options['defaultType'] = 'swap'
            self.exchangeInstance.options['defaultSubType'] = 'linear'
            self.exchangeInstance.load_markets(reload=True)
        elif type == 'spot':
            self.exchangeInstance.options['defaultType'] = 'spot'
            self.exchangeInstance.options['defaultSubType'] = 'linear'
            self.exchangeInstance.load_markets(reload=True)
        else:
            pprint('wrong market type - chose spot or swap')
            sys.exit(1)


    def _loadSymbolFromExchange(self):
        pairData = {}
        for pair in list(self.exchangeMarkets.keys()):
            if pair.find(':') != -1:
                if len(pair[pair.find(':')+1:]) == 4:
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
        #response = None
        historicalFunding = pd.DataFrame()
        startDate = (time.time() - 5259486) * 1000
        limit = 180
        endTime = int(time.time())*1000
        if symbols != None:
            for symbol in symbols:
                urlFunding = f'https://api.bybit.com/v5/market/funding/history?category=linear&limit={limit}&endTime={endTime}&symbol={symbol}USDT'
                symbol = symbol + '/USDT:USDT'
                response = requests.get(urlFunding)
                temp_df = pd.DataFrame(response.json()['result']['list'])
                temp_df.set_index('fundingRateTimestamp', inplace=True)
                temp_df.drop(columns=['symbol'], inplace=True)
                temp_df = temp_df.squeeze().rename(symbol)
                historicalFunding = pd.concat([historicalFunding, temp_df], axis=1)
                print('Bybit: ' + symbol)
            historicalFunding = historicalFunding.sort_index(ascending=True)
            return historicalFunding
        else:
            req_url = requests.get('https://api.bybit.com/v2/public/symbols')
            tickers = req_url.json()['result']
            for symbol in tickers:
                if symbol['quote_currency'] == 'USDT' and symbol['name'].endswith('USDT'):
                    urlFunding = f'https://api.bybit.com/v5/market/funding/history?category=linear&limit={limit}&endTime={endTime}&symbol={symbol["name"]}'#'https://api.bybit.com/v5/market/funding/history?category=linear&limit=180&symbol=' + symbol['name']
                    response = requests.get(urlFunding)
                    temp_df =  pd.DataFrame(response.json()['result']['list'])
                    temp_df.set_index('fundingRateTimestamp', inplace=True)
                    temp_df.drop(columns=['symbol'], inplace=True)
                    if len(temp_df.values) == 1:
                        temp_df = temp_df.iloc[:,0]
                        temp_df.rename(symbol['name'])
                    else:
                        temp_df = temp_df.squeeze().rename(symbol['name'])
                    historicalFunding = pd.concat([historicalFunding, temp_df], axis=1)
                    #pprint(historicalFunding)
                    print('Bybit: ' + symbol['name'])
            historicalFunding = historicalFunding.sort_index(ascending=True)
            return historicalFunding


    def doBuySpot(self, symbol, amount):
        self.changeSPOTtoSWAP('spot')
        return self.exchangeInstance.create_spot_order(symbol=symbol, amount=amount, side='buy', type='market', params={})


    def doBuyPerp(self, symbol, amount):
        self.changeSPOTtoSWAP('swap')
        return self.exchangeInstance.create_market_buy_order(symbol=symbol, amount=amount)


    def doSell(self, data, postOnly):
        if postOnly:
            return self.exchangeInstance.create_limit_sell_order(symbol=data['symbol'], amount=data['amount'],
                                                                price=data['price'],
                                                                params={'time_in_force': 'PostOnly'})
        else:
            return self.exchangeInstance.create_limit_sell_order(symbol=data['symbol'], amount=data['amount'],
                                                                price=data['price'],
                                                                params={})

    def getAccountBalance(self):
        return self.exchangeInstance.fetch_balance()['total']

    def getAccountPosition(self, symbol):
        pos = self.exchangeInstance.fetch_positions(symbols=[symbol])
        return pos

    def getClosedOrders(self, symbol):
        order = self.exchangeInstance.fetch_closed_orders(symbol)
        if len(order) == 0:
            return
        else:
            return order[-1]

    def getTradeFundingHistory(self, symbol, since):
        tradeList = []
        for data in self.exchangeInstance.fetch_my_trades(symbol):
            if data['timestamp'] > since:
                if data['info']['exec_type'] == 'Funding':
                    tradeList.append(data)
            else:
                continue
        return tradeList

    def getOpenOrders(self, symbol):
        return self.exchangeInstance.fetch_open_orders(symbol=symbol)



if __name__ == '__main__':
    e = BybitContainer()
    pprint(e.doBuySpot('BTC/USDT', 5))


