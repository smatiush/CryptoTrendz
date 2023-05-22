import ccxt
from pprint import pprint
import pandas
from datetime import datetime
import pandas as pd
from config.exchangeConfig import OKX_KEY, OKX_SECRET, OKX_PSW
import time
#import zmq
import sys
from ccxt.base.errors import BadSymbol


class OkxContainer():
    def __init__(self):
        self.exchangeId = 'okx'
        self.exchangeInstance = ccxt.okx(
            {
                'apiKey': OKX_KEY,
                'secret': OKX_SECRET,
                'password': OKX_PSW,
                'options':
                    {
                        'enableRateLimit': True,
                        'warnOnFetchFundingFee': False
                    }
            }
        )
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
            raise 'wrong market type - chose spot or swap'

    def _loadSymbolFromExchange(self):
        pairData = {}
        for pair in list(self.exchangeMarkets.keys()):
            if pair.find(':') != -1:
                if len(pair[pair.find(':') + 1:]) == 4:
                    if pair[pair.find(':') + 1:] == 'USDT':
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
        :param symbol: if none fetch all pairs
        :return:
        '''
        self.changeSPOTtoSWAP('swap')
        startDate = int(time.time() - 5259486) * 1000 #5259486 change it if you want to go more back - 5259486s  = 60 day 20 hours 58 minutes
        historicalFunding = pd.DataFrame()
        if symbols is None:
            for symbol in self.pairsData.keys():
                temp = []
                while True:
                    fundingData = self.exchangeInstance.fetch_funding_rate_history(symbol=symbol, since=startDate)
                    if len(temp) == 0:
                        for fund in fundingData:
                            # pprint(fund)
                            temp.append({'fundingRate': fund['fundingRate'], 'timestamp': fund['timestamp']})
                            startDate = int(fund['timestamp'])
                    elif self.exchangeInstance.milliseconds() - startDate < 28800000:#28800000s = 8h
                        temp_df = pd.DataFrame(temp).drop_duplicates()
                        historicalFunding[symbol] = pd.DataFrame(temp_df).set_index('timestamp')
                        # pprint(temp)
                        startDate = int(time.time() - 5259486) * 1000 #reset start date for next pair
                        temp = []
                        break
                    else:
                        for fund in fundingData:
                            # pprint(fund)
                            temp.append({'fundingRate': fund['fundingRate'], 'timestamp': fund['timestamp']})
                            startDate = int(fund['timestamp']) #start date change to be the oldest data in the response from cex
                print('OKX: ' + symbol)
        else:
            for symbol in symbols:
                symbol = symbol + '/USDT:USDT'
                temp = []
                while True:
                    fundingData = self.exchangeInstance.fetch_funding_rate_history(symbol=symbol, since=startDate)
                    if len(temp) == 0:
                        for fund in fundingData:
                            # pprint(fund)
                            temp.append({'fundingRate': fund['fundingRate'], 'timestamp': fund['timestamp']})
                            startDate = int(fund['timestamp'])
                    elif self.exchangeInstance.milliseconds() - startDate < 28800000:
                        temp_df = pd.DataFrame(temp).drop_duplicates()
                        historicalFunding[symbol] = pd.DataFrame(temp_df).set_index('timestamp')
                        #pprint(temp)
                        startDate = int(time.time() - 5259486) * 1000
                        temp = []
                        break
                    else:
                        for fund in fundingData:
                            # pprint(fund)
                            temp.append({'fundingRate': fund['fundingRate'], 'timestamp': fund['timestamp']})
                            startDate = int(fund['timestamp'])
                print('OKX: ' + symbol)
        return historicalFunding

    def getOB(self, symbol):
        return self.exchangeInstance.fetch_order_book(symbol=symbol, limit=5)

    def getOptimalTradeParam(self, pair):
        optimalTradeParam = {}
        #for pair in list(self.pairsData.keys()):
        side = None
        size = None
        price = None
        ob = self.getOB(pair)
        decimalsOB = str(float(ob['bids'][0][0]))[::-1].find('.')
        funding = self.getFunding(pair)
        # pprint (funding)
        if funding['fundingRate'] < 0:
            side = 'long'
        elif funding['fundingRate'] > 0:
            side = 'short'
        elif funding['fundingRate'] == 0:
            side = None
        if side == 'long':
            price = ob['bids'][0][0]
            size = ob['bids'][0][1]
        elif side == 'short':
            price = ob['asks'][0][0]
            size = ob['asks'][0][1]
        optimalTradeParam[pair] = {'cex': self.exchangeId, 'side':side, 'size':size, 'price':price}
        #pprint(optimalTradeParam)
        return optimalTradeParam

    def doBuy(self, data, postOnly):
        if postOnly:
            return self.exchangeInstance.create_limit_buy_order(symbol=data['symbol'], amount=data['amount'],
                                                                price=data['price'],
                                                                params={'time_in_force': 'PostOnly'})
        else:
            return self.exchangeInstance.create_limit_buy_order(symbol=data['symbol'], amount=data['amount'],
                                                                price=data['price'],
                                                                params={})

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
        try:
            balance = self.exchangeInstance.fetch_total_balance()
            return balance
        except Exception as e:
            raise e
    def getFundingPayments(self, symbol):
        self.exchangeInstance.fetch_funding_history(symbol)

    def getAccountPosition(self, symbol):
        if type(symbol) == list:
            return self.exchangeInstance.fetch_positions(symbols=symbol)
        else:
            return self.exchangeInstance.fetch_position(symbol=symbol)

    def getClosedOrders(self, symbol, since, type):#SWAP - SPOT
        order = None
        if type == 'swap':
            self.changeSPOTtoSWAP('swap')
            order = self.exchangeInstance.fetch_my_trades(symbol, since=since)
            pprint(order)
        elif type == 'spot':
            #self.changeSPOTtoSWAP('spot')
            order = self.exchangeInstance.order(symbol=symbol)
            pprint(order)
        if len(order) == 0 or order == None:
            return
        else:
            return order

    def getOpenOrders(self, symbol):
        return self.exchangeInstance.fetch_open_orders(symbol=symbol)

    def getFarmingPosition(self):
        pair = self.getAccountBalance()
        farmingPosition = []
        for symbol in pair.keys():
            if symbol == 'USDT':
                continue
            else:
                ticker = symbol + '/USDT:USDT'
                try:
                    perpPosition = e.getAccountPosition(ticker)
                    if perpPosition != None:
                        pprint(perpPosition['timestamp'])
                        spotBalance = pair[symbol]
                        pprint(self.getClosedOrders(symbol=ticker, type='swap', since=perpPosition['timestamp'] - 86400000))
                    else:
                        continue
                except BadSymbol as error:
                    pprint(error)
                    continue



if __name__ == '__main__':
    e = OkxContainer()
    e.getClosedOrders(symbol='FITFI/USDT:USDT', type='spot', since=1683190672000 - 86400000)


