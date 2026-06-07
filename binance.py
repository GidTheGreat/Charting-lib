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
spot_queue = asyncio.Queue(maxsize=1000)
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
        df = f"df_{symbol}"
        markets.add(symbol)
        market_data[symbol]["tick_data"].append(
    {"aggressor":aggressor, 
    "volume": quantity, 
    "epoch":epoch,
    "quote":price}) 
        if rollover:
            rollover = False
            ticks = market_data[symbol]["tick_data"]
            if ticks:
                start = ticks[0]["epoch"]
                end = ticks[-1]["epoch"]
                df = pd.DataFrame(market_data[symbol]["tick_data"])
                df.to_csv(f"{symbol}_{start}_{end}.csv",index=False)
        flux.flux(f"{symbol}",
                     f"aggressor :{aggressor}, "
                     f"volume :{quantity}, market price :"
                     f"{price}, time :"
                     f"{datetime.fromtimestamp(epoch)},"
                     f"{symbol},current time :"
                     f"{datetime.now()}")
        await asyncio.sleep(0.1)

async def spot_stream():
    """Establish a web socket connection to binance
    for fut trades
    """
    asyncio.create_task(handle_stream_spot())
    try:
        async with connect(
        "wss://stream.binance.com:9443/stream?streams=btcusdt@aggTrade/ethusdt@aggTrade/btcusdc@aggTrade") as ws_spot:
            flux.flux("spot stream connection","successful")
            while True:
                msg_spot = json.loads(await ws_spot.recv())
                await spot_queue.put(msg_spot)
                await asyncio.sleep(0.1)
                
    except Exception:
         print("spot_stream_error",
         traceback.print_exc())


async def main():
    """Establish a websocket connection
       with binance and populate queue
    """
    await asyncio.gather(day_rollover(),
    spot_stream())
    
    try:
        pass
        
                
    except Exception:
        print("Main program error",traceback.print_exc()) 
    finally:   
        print(" Program Ending")
        if markets:
            for symbol in markets:
                start = market_data[symbol]["tick_data"][0]["epoch"]
                end = market_data[symbol]["tick_data"][-1]["epoch"]
                df = pd.DataFrame(market_data[symbol]["tick_data"])
                df.to_csv(f"{symbol}_{start}_{end}.csv",index=False)
        print("saved")
    
        
asyncio.run(main())    
