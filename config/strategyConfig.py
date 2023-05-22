#MARKET PARAM CONFIG
ALLOWED_CEX = ['okx', 'bybit', 'bitmex']
ALLOWED_SYMBOL = None#['BTC', 'ETH', 'DOGE', 'SOL', 'ADA', 'AVAX'] # if set 'None' all pairs will be taken

#STRATEGY PARAM
TIMEFRAME = 'D' # D = day, W= week, M=month, Y=year
TOT_POSITION_CEX = 2

def getConfig():
    return\
        {
            'ALLOWED_CEX' : ALLOWED_CEX,
            'ALLOWED_SYMBOL' : ALLOWED_SYMBOL,
            'TIMEFRAME' : TIMEFRAME,
            'TOT_POSITION_CEX' : TOT_POSITION_CEX
        }