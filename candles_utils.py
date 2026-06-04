"""This module takes tick data and makes candle data.

COMMENTS FOR ME
Note this algo works best on tf_secs 60secs....have not tested it on other tfs
....if to add more tf functionality....note last_complete_index

ASSUMPTION: NO TWO ENTRIES CAN BE EQUIVALENT BECAUSE OF EPOCH
"""

import asyncio
#import time
from collections import defaultdict
from datetime import datetime

import pandas as pd


async def handle_candles(symbol,market_data):
    """Tick data is made into candle level data here
    """
    i = 0
    timeframes = {"1min":60}
    forming_candle = {}
    #track intervals where close is recorded and thus remove them from next interval processing
    last_complete_index = 0
    #to ensure open and close prices are from same interval
    current_bin = None
    #make generator eternal
    while True:
        #cpu_start = time.perf_counter()
        tick_data = market_data[symbol]["tick_data"]
        print(datetime.now())
        while i < len(tick_data) and len(tick_data) >2:
          #ensure cursor is less than len of tick data before processing
            i = len(tick_data)
            #latest unprocessed slice
            current_slice = tick_data[last_complete_index:]
            for tf, tf_secs in timeframes.items():
                for idx,entry in enumerate(current_slice):
                    if tf_secs == 60:
                        forming_candle["count"] = datetime.fromtimestamp(entry["epoch"]).second
                    #to provent weird case of idx being -1
                    if idx == 0:
                        continue
                    if ((entry["epoch"] // tf_secs) !=
                    (current_slice[idx-1]["epoch"] // tf_secs)):
                        assert (
                        entry != current_slice[idx-1]
                        ), "Two entries in tickdata are equal ,REASSESS"
                        #print("entry:",datetime.fromtimestamp(entry["epoch"]))
                        forming_candle["open"] = entry["quote"]
                        forming_candle["epoch"] = entry["epoch"]
                        #to record bin of open price for later comparison
                        current_bin = entry["epoch"] // tf_secs
                        #get all the values from start price to end of bin and find max and min
                        forming_candle["high"] = max(
                        (tick["quote"] for tick in current_slice[idx:]
                        if tick["epoch"] // tf_secs == current_bin)
                        )
                        forming_candle["low"] = min(
                        (tick["quote"] for tick in current_slice[idx:]
                        if tick["epoch"] // tf_secs == current_bin)
                        ) 
                    elif (
                      ((idx+1) < (len(current_slice)-1)) and (entry["epoch"] // tf_secs) !=
                    (current_slice[idx+1]["epoch"] // tf_secs)
                    ):
                        assert (
                        entry != current_slice[idx+1]
                        ), "Two entries in tickdata are equal ,REASSESS"
                        #ensure close price belongs to same bin as open price
                        if entry["epoch"] // tf_secs != current_bin:
                            #print("INCOMPLETE CANDLE SKIPPING")
                            continue                    
                        forming_candle["close"] = entry["quote"]
                        last_complete_index = tick_data.index(entry)
                        print(forming_candle)
                        del forming_candle["count"]
                        market_data[symbol][f"candles_{tf}"].append(forming_candle)
                        #dsp.flux(forming_candle)
                        #print(market_data[symbol][f"candles_{tf}"])
                        forming_candle ={}
                        current_bin = None               
            yield i,forming_candle
        await asyncio.sleep(0.1)

  
async def run(symbol,market_data):
    """Handle generator of candles
    """
    async for i,forming_candle in handle_candles(symbol,market_data):
        print(forming_candle["count"],"s",i)

  
async def main():
    """Entry point
    """
    market_data = defaultdict(lambda:defaultdict(list))
    asyncio.create_task(run("R",market_data))
    df = pd.read_csv("test.csv",index_col=0)
    lst = df.to_dict(orient="records")
    lst = lst[0:75]
    for item in lst:
        market_data["R"]["tick_data"].append(item)
        await asyncio.sleep(0.1)

   
if __name__ == "__main__":
    asyncio.run(main())
    