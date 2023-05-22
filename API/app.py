from flask import Flask, request
from flask.views import View

import sys
sys.path.append('../FundingFarm')
from DataEngine.DButils.DBengine import DataBaseEngine
from DataEngine.dataManager import pollerManager
from config import strategyConfig
from pprint import pprint
import json

app = Flask(__name__)
#pollerManagerClass = pollerManager()
#pollerData = pollerManagerClass.updatePoller()
dataBaseEngine = DataBaseEngine()

def readJson():
    import os
    #print(os.getcwd())
    with open(str(os.getcwd()) + "/API/temp_data.json", 'r') as outfile:
        jData = json.load(outfile)
        return jData

@app.route("/tempAggregatedData")
def getTempData():
    return dataBaseEngine.search_funding_rate()

@app.route('/timestampLastData', methods=['GET'])
def getTimestampLastData():
    key = list(readJson()[-1].keys())[0]
    return {'timestamp': str(readJson()[-1][key]['timestamp_recv'])}

'''@app.route("/strategyConfig", methods=['GET'])
def getStrategyConfig():
    return strategyConfig.getConfig()'''

@app.route("/fundingPerformance", methods=['GET'])
def getFundingPerformance():
    pollerData = readJson()
    args = request.args
    cex = args.get('cex', type = str)
    return json.dumps(pollerData[-1][cex]['fundingPerformance'])

@app.route("/fundingRank", methods=['GET'])
def getFundingRanked():
    pollerData = readJson()[-1]
    args = request.args
    cex = args.get('cex', type=str)
    #cex = 'okx'
    finalRank = []
    sorted_rank_descending = sorted(pollerData[cex]['fundingRank'].items(), key=lambda x: x[1], reverse=True)
    for pairData in sorted_rank_descending:
        finalRank.append({'pair': pairData[0], 'funding': pairData[1]})
    return finalRank




if __name__ == "__main__":
    #pprint(getFundingRanked())
    app.run(host='0.0.0.0')