import datetime
import os
import sys
import pandas as pd
import functools
sys.path.append('../FundingFarm')
import time
from DataEngine import dataEngine
#from DataEngine.DButils.DBManager import DBmanager
from config.strategyConfig import ALLOWED_CEX, ALLOWED_SYMBOL
import json
from pprint import pprint

class pollerManager():

    def __init__(self):
        self.ALLOWED_CEX = ALLOWED_CEX
        self.ALLOWED_SYMBOL = ALLOWED_SYMBOL
        self.engineClass = dataEngine.Engine()
        #self.DBmanager = DBmanager()
        self.marketsSymbol = self.engineClass.allPairs
        self.baseSymbolFifo = self.engineClass.baseSymbol
        self.historicalData = None
        self.positionData = {}
        self.fifo = []


    def _reloadHistoricalData(self):
        self.historicalData = self.engineClass.getHistoricalFunding()

    def getHistoricalRankedData(self):
        '''
        idea sortare dal più grande al più piccolo e selezionare i primi 5
        :return:
        '''
        self._reloadHistoricalData()
        data = self.historicalData
        fundingRank = {}
        for cex in data:
            sorted = data[cex].sum().sort_values(ascending=False)
            filtered_return = sorted.rank(ascending=False)
            filter1 = filtered_return.values < 5
            filtered_rank = filtered_return[list(filter1)].index
            dataRank = data[cex][filtered_rank].sum()
            fundingRank[cex] = dataRank.to_dict()
        return fundingRank


    def getHistoricalPerformanceData(self):
        '''
            Performance calculation of historical data
            rank the fundingRates (already resampled based on TIMEFRAME in strategyConfig.py) from the lowest to highest - pairFundingLower == 0 , pairFundingMax == len(pairNameList).
            calculate the SMA of ranking to retrieve the pair that had the max funding most of the time.
        '''
        historicalData = {}
        data = self.historicalData  #data = df per ogni exchange con i funding formattati in base al TIMEFRAME in strategyConfig.py
        maxFundingPerf = {}
        minFundingPerf = {}
        for cex in data:
            maxFundingPerf[cex] = []
            minFundingPerf[cex] = []
            rank = data[cex].rank(axis=1, method='average', ascending=False)
            performanceRanking = rank.mean()
            performanceRanking = performanceRanking.sort_values(axis=0, ascending=False)

            filterMax = performanceRanking.keys().to_list()

            filterMax = filterMax[:5]
            for symbol in filterMax:
                maxFundingPerf[cex].append({'symbol':symbol, 'fundingRate' : data[cex][symbol].sum(), 'performanceParam': performanceRanking[symbol]})

            filterMin = performanceRanking.keys().to_list()
            filterMin = filterMin[len(filterMin)-5:]
            for symbol in filterMin:
                minFundingPerf[cex].append({'symbol':symbol, 'fundingRate' : data[cex][symbol].sum(), 'performanceParam': performanceRanking[symbol]})
            historicalData[cex] = {
                'maxFunding' : minFundingPerf[cex],
                'minFunding': maxFundingPerf[cex]
            }
        return historicalData

    def getFundingData(self):
        fundingStats = {}
        data = self.engineClass.getMarketData(self.marketsSymbol)
        for cex in data:
            if cex not in fundingStats.keys():
                fundingStats[cex] = {}
            for pair in data[cex]:
                print(cex, pair)
                if pair not in fundingStats[cex].keys():
                    fundingStats[cex][pair] = {}
                fundingStats[cex][pair]['cex'] = data[cex][pair]['cex']
                fundingStats[cex][pair]['pair'] = data[cex][pair]['tradingSymbol']
                fundingStats[cex][pair]['fundingInfo'] = data[cex][pair]['fundingInfo']
                fundingStats[cex][pair]['timestamp_recv'] = time.time()
        return fundingStats

    def updatePoller(self):
        '''
        aggregatore di tutti i dati provenienti dai CEX e li aggrega in un dizionario
        :return: {'fundingData' : {CEX_NAME: {fundingRate, nextFundingRate, timestamp}}}
        '''
        self._reloadHistoricalData()
        timestamp = str(int(time.time()))
        aggregatedData = {}
        historicalPerformanceData = self.getHistoricalPerformanceData()
        historicalRankedData = self.getHistoricalRankedData()
        balances = self.engineClass.getBalances()
        for cex in  historicalPerformanceData.keys():
            aggregatedData[cex] = {
                'fundingRank' : historicalRankedData[cex],
                'fundingPerformance': historicalPerformanceData[cex],
                'balances': balances[cex],
                'timestamp_recv': time.time()
                }
        return aggregatedData

    def updateJson(self):
        isTempFileCreated = os.path.isfile('./API/temp_data.json')
        pprint(isTempFileCreated)
        if isTempFileCreated:
            print('UPDATE JSON DATA')
            data = self.updatePoller()
            with open(str(os.getcwd()) + "/API/temp_data.json", "r+") as outfile:
                temp_data = json.load(outfile)
                if len(temp_data) >= 21: # 21 = tot funding rate in a week
                    temp_data.append(data)
                    temp_data.pop(0)
                    outfile.seek(0)
                    json.dump(temp_data, outfile, indent=3)
                    outfile.truncate()
                else:
                    temp_data.append(data)
                    outfile.seek(0)
                    json.dump(temp_data, outfile, indent=3)
                    outfile.truncate()
            print('JSON DATA UPDATED SUCCESSFULLY')
        elif isTempFileCreated != True:
            initial_data = []
            print("LOADING INITIAL DATA")
            data = c.updatePoller()
            initial_data.append(data)
            # pprint(initial_data)
            with open(str(os.getcwd()) + "/API/temp_data.json", "w+") as out_file:
                outData = json.dumps(initial_data, indent=3)
                out_file.write(outData)
            print("FINISH LOADING INITIAL DATA")

    def updateDB(self):
        self.DBmanager.do_db_operation()



if '__main__' == __name__:
    import pytz
    '''funding_time = [00,8,16]
    c = pollerManager()
    pprint(c.updatePoller())
    #while True:
    timeNow = datetime.datetime.now(tz=pytz.UTC)
    print(timeNow.hour, timeNow.minute, timeNow.second)
    #while timeNow.hour in funding_time and timeNow.minute <= 2:
    print(timeNow.hour, timeNow.minute, timeNow.second)
    #c.updateJson()
    c.updateDB()
    time.sleep(15)'''
        #time.sleep(1)
    # _______________PRODUCTION CODE____________________
    # **********REMOVE COMMENT ONLY BEFORE FLIGHT*******

    funding_time = [00,8,16]
    c = pollerManager()
    while True:
        timeNow = datetime.datetime.now(tz=pytz.UTC)
        print(timeNow.hour, timeNow.minute, timeNow.second)
        while timeNow.hour in funding_time and timeNow.minute <= 3:
            print(timeNow.hour, timeNow.minute, timeNow.second)
            #c.updateJson()
            c.updateDB()
            time.sleep(1)
        time.sleep(1)


