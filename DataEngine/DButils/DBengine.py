import pymongo
from pymongo import MongoClient
import sys
from pprint import pprint
import pandas as pd
import time
sys.path.append('./../FundingFarm')
import config.botConfig as botConfig

class DataBaseEngine():
    def __init__(self):
        self.cluster = MongoClient("mongodb://139.59.154.21:27017", username=botConfig.DB_USER, password=botConfig.DB_PASSWORD)
        self.db = self.cluster[botConfig.DATABASE]
        self.collectionFunding = self.db['historical_funding_rate']
        self.collectionRank = self.db['return_funding_rank']

    def insert_funding_rate(self, data):
        pprint(self.collectionFunding.insert_one(data))

    def update_funding_rate(self, data):
        dataCex = data
        for cex in dataCex.keys():
            dataDb = dataCex[cex]
            for symbol in dataDb.keys():
                print(cex, symbol)
                dataDb.index = dataDb[symbol].index.astype(str) #dataDb[symbol].index.strftime('%Y-%m-%d %H:%M:%S')
            self.collectionFunding.replace_one({'_id': cex}, dataDb.to_dict(), upsert=False)
                #pprint(documentCEX)
    def search_funding_rate(self):
        rawFundingRate = {}
        for data in self.collectionFunding.find():
            cex = data['_id']
            data.pop('_id', None)
            rawFundingRate[cex] = pd.DataFrame(data.copy())
        return rawFundingRate

    def update_simple_rank(self, data):
        data.update({'_id': time.time()})
        pprint('Update Simple Rank')
        self.collectionRank.insert_one(data)

    def search_simple_rank(self):
        rank = {}
        for data in self.collectionRank.find():
            rank.update(data)
        return rank

if __name__ == '__main__':
    d = DataBaseEngine()
    d.search_simple_rank()