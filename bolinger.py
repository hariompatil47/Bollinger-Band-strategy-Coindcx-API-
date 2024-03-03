import requests
import pandas as pd
import datetime as dt
import time
import talib as ta
import hmac
import hashlib
import base64
import json


with open("cread.json","r") as file:
    cread = json.load(file)


# Enter your API Key and Secret here.
key = cread['api_key']
secret = cread['api_secret']


# python3
secret_bytes = bytes(secret, encoding='utf-8')


# Generating a timestamp.
timeStamp = int(round(time.time() * 1000))


# Download Symbol name And Convert
url = "https://api.coindcx.com/exchange/v1/markets"
response = requests.get(url)
data = response.json()


Currency = "INR"


inr_pair = []
for i in data:
    if i[-3:] == Currency:
        x = i[:-3] +"_" +i[-3:]
        inr_pair.append(x)


# Data Downloader
# Data Downloader
def data_downloader(name, interval):
    url = f"https://public.coindcx.com/market_data/candles?pair=I-{name}&interval={interval}&limit=20"
    response = requests.get(url)
    data = response.json()
    df = pd.DataFrame(data)
    df['date_time'] = df['time'].apply(lambda x: time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(int(str(x)[:-3]))))
    df['date_time'] = pd.to_datetime(df['date_time'])
    df = df.sort_values(by='date_time')
    df = df.reset_index(drop=True)
    df = df[['date_time', 'open', 'high', 'low', 'close', 'volume']]
    
    # Convert problematic columns to Python integers
    df[['open', 'high', 'low', 'close', 'volume']] = df[['open', 'high', 'low', 'close', 'volume']].astype(int)
    
    df[['open', 'high', 'low', 'close', 'volume']] = df[['open', 'high', 'low', 'close', 'volume']].astype(int)  # Convert int32 to int
    df['upperband'], df['middleband'], df['lowerband'] = ta.BBANDS(df.close, timeperiod=20, nbdevup=2, nbdevdn=2, matype=0)
    return df




# Order Function
def place_order(side, symbol, price, qty):
    # Convert qty to a regular Python integer
    qty = int(qty)

    # Ensure Quantity is greater than zero
    if qty <= 0:
        print("Invalid quantity. Skipping order placement.")
        return None

    body = {
        "side": side,
        "order_type": "limit_order",
        "market": symbol,
        "price_per_unit": price,
        "total_quantity": qty,
        "timestamp": timeStamp
    }
    print("Body:", body)  # Print the body dictionary
    json_body = json.dumps(body, separators=(',', ':'))
    signature = hmac.new(secret_bytes, json_body.encode(), hashlib.sha256).hexdigest()
    url = "https://api.coindcx.com/exchange/v1/orders/create"
    headers = {
        'Content-Type': 'application/json',
        'X-AUTH-APIKEY': key,
        'X-AUTH-SIGNATURE': signature
    }
    response = requests.post(url, data=json_body, headers=headers)
    data = response.json()
   
    return data







trade_cap = 250  # Default value
status = {x : {"traded": None, 'qty': None} for x in inr_pair}


# Scan Pair According to Bollinger Bands
while True:
    for pair in inr_pair:
        df = data_downloader(pair, "5m")
        close = df.iloc[-1]['close']  # Close Value Of Last Candle
        upper_band = df.iloc[-1]['upperband']
        lower_band = df.iloc[-1]['lowerband']
       
        # Sell Side
        if close > upper_band and status[pair]['traded'] == None:
            pair1 = pair.replace("_","")
            Quantity = int(trade_cap / close)
            res = place_order("sell", pair1, close, Quantity)
            print(f"{pair} Sell Trade at {close}  {dt.datetime.now().time()}")
            status[pair]['traded'] = "Bought"
            status[pair]['qty'] = Quantity


        # Buy Side
        if close < lower_band and status[pair]['traded'] == None:
            pair2 = pair.replace("_","")
            Quantity = int(trade_cap / close)
            res = place_order("buy", pair2, close, Quantity)
            print(f"{pair} Buy Trade at {close}  {dt.datetime.now().time()}")
            status[pair]['traded'] = "Sold"
            status[pair]['qty'] = Quantity


        # Buy Exit
        if status[pair]['traded'] == "Bought" and close < upper_band:
            pair2 = pair.replace("_","")
            qty1 = status[pair]['qty']
            res = place_order("sell", pair2, close, qty1)
            print(f"{pair} Buy Trade Exit at {close}  {dt.datetime.now().time()}")
            status[pair]['traded'] = None
            status[pair]['qty'] = None


        # Sell Exit
        if status[pair]['traded'] == "Sold" and close > lower_band:
            pair2 = pair.replace("_","")
            qty2 = status[pair]['qty']
            res = place_order("buy", pair2, close, qty2)
            print(f"{pair} Sell Trade Exit at {close}  {dt.datetime.now().time()}")
            status[pair]['traded'] = None

