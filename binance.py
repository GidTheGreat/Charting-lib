"""
The binance exchange websocket streamer for the charting app.

BINANCE SHORTHAND:
    e: event type
    E: event time(time trade is broadcast)
    s: symbol
    t: trade ID
    p: price
    q: quantity traded
    T: trade execution time(time trade actually happened)
    m: was the buyer market maker i.e. 
    if True a sell market order hit a buy
     limit order,if False buy market order
      hit a sell limit order
    
    if aggTrade:
        a: aggregate trade ID
        f: first trade ID in this aggreagte
        l: last trade ID in this aggreagte
        

"""

import time
import math
import sys
import json
import asyncio
import traceback
import pandas as pd
from datetime import datetime
from collections import defaultdict,deque

from websockets.asyncio.client import connect
from fluxdeck import FluxDeck

from candles_utils import handle_candles

markets = set()
market_data = defaultdict(lambda:defaultdict(list))
flux = FluxDeck()
spot_queue = asyncio.Queue(maxsize=20000)
fut_queue = asyncio.Queue(maxsize=1000)
last_day = datetime.now().day
rollover = False

async def day_rollover():
    global last_day, rollover
    while True:
        now = datetime.now()
        if now.day != last_day:
            print("day change detected")
            rollover =True
            last_day = now.day
        await asyncio.sleep(45)    

def save_file():
    for symbol in markets:
        ticks = market_data[symbol]["tick_data"]
        if ticks:
            start = ticks[0]["epoch"]
            end = ticks[-1]["epoch"]
            df = pd.DataFrame(market_data[symbol]["tick_data"])
            df.to_csv(f"{symbol}_{start}_{end}.csv",index=False)
        
async def handle_stream_spot():
    """Read websocket message from queue and populate market_data
    """
    global rollover
    while True:
        stream = await spot_queue.get()
        stream_data = stream["data"]
        aggressor = "seller" if stream_data["m"] else "buyer"
        quantity = stream_data["q"]
        price = stream_data["p"]
        #epoch converted to s from ms
        epoch = math.floor(stream_data["T"]/1000)
        symbol = stream_data["s"]
        markets.add(symbol)
        market_data[symbol]["tick_data"].append(
    {"aggressor":aggressor, 
    "volume": quantity, 
    "epoch":epoch,
    "quote":price}) 
        if rollover:
            rollover = False
            save_file()
        """   
        flux.flux(f"{symbol}",f"queue size :{spot_queue.qsize()},"
                     f"aggressor :{aggressor}, "
                     f"volume :{quantity}, market price :"
                     f"{price}, time :"
                     f"{datetime.fromtimestamp(epoch)},"
                     f"{symbol},current time :"
                     f"{datetime.now()}")"""
        

async def spot_stream():
    """Establish a web socket connection to binance
    for fut trades
    """
    asyncio.create_task(handle_stream_spot())
    try:
        async with connect(
        "wss://stream.binance.com:9443/stream?streams=btcusdt@aggTrade/ethusdt@aggTrade/btcusdc@aggTrade",
        ping_interval=20,
    ping_timeout=20,
    max_queue=None
    ) as ws_spot:
            flux.flux("spot stream connection","successful")
            while True:
                msg_spot = json.loads(await ws_spot.recv())
                await spot_queue.put(msg_spot)
                
    except Exception:
         print("spot_stream_error",
         traceback.print_exc())
async def heartbeat():
    while True:
        if markets:
            print("still alive,current market data len:",len(market_data[list(markets)[0]]["tick_data"]))
        else:
            print("starting up")
        await asyncio.sleep(60)

async def main():
    """Establish a websocket connection
       with binance and populate queue
    """
    
    try:
        await asyncio.gather(day_rollover(),
    spot_stream(),heartbeat())
                
    except Exception:
        print("Main program error",traceback.print_exc()) 
    finally:   
        print(" Program Ending")
        if markets:
            save_file()
        print("saved")
    
        
asyncio.run(main())    
