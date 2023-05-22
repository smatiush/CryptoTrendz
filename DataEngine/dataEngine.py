
# setting path
import time
import pandas as pd
from exchange.bybit import BybitContainer
from exchange.okx import OkxContainer
from exchange.bitmex import BitmexContainer
from config.strategyConfig import ALLOWED_SYMBOL, ALLOWED_CEX, TIMEFRAME
from pprint import pprint
from DataEngine.DButils.DBengine import DataBaseEngine


class Engine():
    def __init__(self):
        self.ALLOWED_CEX = ALLOWED_CEX
        self.ALLOWED_SYMBOL = ALLOWED_SYMBOL
        self.exchangeClassContainer = {
            'bybit': BybitContainer(),
            'okx':OkxContainer(),
            'bitmex':BitmexContainer()
        }
        self.tradableSymbol = {}
        self.allPairs = {}
        self.baseSymbol = []
        self.DBengine = DataBaseEngine()
        for cex in self.exchangeClassContainer.keys():
            self.allPairs[cex] = []
            for pair in self.exchangeClassContainer[cex].pairsData.keys():
                if pair[pair.find(':') + 1:] == 'USDC' or 'USDT' or 'USD':
                    if self.ALLOWED_SYMBOL == None or pair[:pair.find('/')] in self.ALLOWED_SYMBOL:
                        self.allPairs[cex].append(pair)
                else:
                    continue

    def getPerpPair(self):
        self.tradableSymbol = {}
        self.allPairs = {}
        self.baseSymbol = []
        for cex in self.exchangeClassContainer.keys():
            self.allPairs[cex] = []
            for pair in self.exchangeClassContainer[cex].pairsData.keys():
                if pair[pair.find(':') + 1:] == 'USDC' or 'USDT' or 'USD':
                    if self.ALLOWED_SYMBOL == 'all' or pair[:pair.find('/')] in self.ALLOWED_SYMBOL:
                        self.allPairs[cex].append(pair)
                else:
                    continue

    def getBalances(self):
        balances = {}
        for cex in self.exchangeClassContainer.keys():
            balances[cex] = self.exchangeClassContainer[cex].getAccountBalance()
        return balances

    def getRawFundingRate(self):
        historical_funding = {}
        for cex in self.exchangeClassContainer.keys():
            if self.ALLOWED_SYMBOL == None:
                df = self.exchangeClassContainer[cex].getHistoricalFunding()
            else:
                df = self.exchangeClassContainer[cex].getHistoricalFunding(self.ALLOWED_SYMBOL)
            df = df.astype(float)
            # df.index = pd.to_numeric(df.index)
            df.index = pd.to_datetime(df.index, unit='ms')
            df = df * 100
            historical_funding[cex] = df
            print('getRawFunfing')
            pprint(len(df))
        return historical_funding

    def getHistoricalFunding(self):
        historical_funding = {}
        FundData = self.DBengine.search_funding_rate()
        for cex in FundData.keys():
            if self.ALLOWED_SYMBOL == None:
                df = FundData[cex]
                df.index = pd.to_datetime(df.index)

            else:
                df = self.exchangeClassContainer[cex].getHistoricalFunding(self.ALLOWED_SYMBOL)
                df = df.astype(float)
                #df.index = pd.to_numeric(df.index)
                df.index = pd.to_datetime(df.index, unit='ms')
                df = df * 100
            return1D = df.resample('D').sum().sort_index()

            if TIMEFRAME == 'D':
                return1D.drop(index=list(return1D.index)[-1], inplace=True)
                historical_funding[cex] = return1D
            elif TIMEFRAME == 'W':
                returnW = return1D.resample('W').sum().sort_index()
                returnW.drop(index=list(returnW.index)[-1], inplace=True)
                historical_funding[cex] = returnW
            elif TIMEFRAME == 'M':
                returnM = return1D.resample('M').sum().sort_index()
                returnM.drop(index=list(returnM.index)[-1], inplace=True)
                historical_funding[cex] = returnM
            #pprint(historical_funding)
        return historical_funding

    def getMarketData(self, allowedMarkets):
        tradingPairsStats = {}
        for cex, pairs in allowedMarkets.items():
            if cex not in tradingPairsStats.keys():
                tradingPairsStats[cex]= {}
            for pair in pairs:
                tradingPairsStats[cex][pair] = {
                    'tradingSymbol': pair,
                    'cex': str(cex),
                    'fundingInfo': self.exchangeClassContainer[cex].getFunding(pair),
                    'timestamp': time.time()
                }
        return tradingPairsStats

    def getFundingPayments(self):
        pass


if '__main__' == __name__:
    e = Engine()
    pprint(e.getRawFundingRate())
