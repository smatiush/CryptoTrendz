import sys
import time
import pandas as pd
from datetime import datetime
import pytz
sys.path.append('./../FundingFarm')
from DataEngine.dataEngine import Engine
from DataEngine.dataManager import pollerManager
from DataEngine.DButils.DBengine import DataBaseEngine
from pprint import pprint

class DBmanager():
    def __init__(self):
        self.dataEngine = Engine()
        self.dataManager = pollerManager()
        self.dbEngine = DataBaseEngine()

    def insert_hist_raw_funding(self):
        histFunding = self.dataEngine.getRawFundingRate()
        for cex in histFunding.keys():
            histFunding[cex].index = histFunding[cex].index.strftime('%Y-%m-%d %H:%M:%S')
            dataDb = { '_id' : cex }
            print(cex + 'insert in db')
            pprint(len(histFunding[cex]))
            funding_temp = histFunding[cex].to_dict()
            dataDb.update(histFunding[cex].to_dict())
            self.dbEngine.insert_funding_rate(dataDb)

    def update_hist_raw_funding(self):
        dataF = self.dataEngine.getRawFundingRate()
        for cex in dataF.keys():
            dataF[cex].index = dataF[cex].index.astype(str)
            self.dbEngine.update_funding_rate(dataF)
            print('UPDATE rawHistFunding: COMPLETE')

    def search_raw_hist_funding(self):
        return self.dbEngine.search_funding_rate()

    def update_simple_rank(self):
        ranked = self.dataManager.getHistoricalRankedData()
        self.dbEngine.update_simple_rank(ranked)

    def do_db_operation(self):
        histFundData = self.dbEngine.search_funding_rate()
        simpleRank = self.dbEngine.search_simple_rank()
        if len(histFundData) == 0:
            print('inserting historical raw funding data')
            self.insert_hist_raw_funding()
        else:
            print('updating historical raw funding data')
            self.update_hist_raw_funding()
        if len(simpleRank) == 0:
            print('inserting simple rank')
            self.update_simple_rank()
        else:
            print('updating simple rank')
            self.update_simple_rank()


if __name__ == '__main__':
    # _______________PRODUCTION CODE____________________
    # **********REMOVE COMMENT ONLY BEFORE FLIGHT*******

    funding_time = [00, 8, 16]
    c = DBmanager()
    while True:
        timeNow = datetime.now(tz=pytz.UTC)
        print(timeNow.hour, timeNow.minute, timeNow.second)
        while timeNow.hour in funding_time and timeNow.minute <= 3:
            print(timeNow.hour, timeNow.minute, timeNow.second)
            # c.updateJson()
            c.do_db_operation()
            time.sleep(1)
        time.sleep(1)
