import asyncio
import pandas as pd
from datetime import datetime
import time
from collections import defaultdict

"""Note this algo works best on tf_secs 60secs....have not tested it on other tfs....if to add more tf functionality....note last_complete_index"""


"""ASSUMPTION: NO TWO ENTRIES CAN BE EQUIVALENT BECAUSE OF EPOCH"""

dt = datetime.fromtimestamp


async def handle_candles(symbol,market_data,dsp=None): 
    i = 0
    candles = []
    timeframes = {"1min":60}
    forming_candle = {}
    last_complete_index = 0 # to keep track of intervals where close price is recorded and thus remove them from next interval processing
    current_bin = None #to ensure open and close prices are from same interval
       
    while True: # make generator eternal
        cpu_start = time.perf_counter()
        tick_data = market_data[symbol]["tick_data"]
        print(datetime.now())
        
        while i < len(tick_data) and len(tick_data) >2: # ensure cursor is less than len of tick data before processing
            i = len(tick_data)
            current_slice = tick_data[last_complete_index:] # latest unprocessed slice
            
            for tf, tf_secs in timeframes.items():
                for idx,entry in enumerate(current_slice):
                    if tf_secs == 60:
                        forming_candle["count"] = datetime.fromtimestamp(entry["epoch"]).second
                    if idx == 0: # to provent weird case of idx being -1
                        continue
                    
                    #print("index:",idx,"entry:",entry["quote"],datetime.fromtimestamp(entry["epoch"]))
                    
                    if entry["epoch"] // tf_secs != current_slice[idx-1]["epoch"] // tf_secs:
                        assert entry != current_slice[idx-1], "Two entries in tickdata are equal ,REASSESS"
                        #print("entry:",datetime.fromtimestamp(entry["epoch"]))
                        forming_candle["open"] = entry["quote"]
                        forming_candle["epoch"] = entry["epoch"]
                        current_bin = entry["epoch"] // tf_secs # to record bin of open price for later comparison
                        
                        #get all the values from start price to end of bin and find max and min
                        forming_candle["high"] = max([tick["quote"] for tick in current_slice[idx:] if tick["epoch"] // tf_secs == current_bin])
                        forming_candle["low"] = min([tick["quote"] for tick in current_slice[idx:] if tick["epoch"] // tf_secs == current_bin])                        
                        
                        
                        
                    elif ((idx+1) < (len(current_slice)-1)) and entry["epoch"] // tf_secs != current_slice[idx+1]["epoch"] // tf_secs:
                        assert entry != current_slice[idx+1], "Two entries in tickdata are equal ,REASSESS"
                        if entry["epoch"] // tf_secs != current_bin: # to ensure close price belongs to same bin as open price
                            #print("INCOMPLETE CANDLE SKIPPING")
                            continue
                        #print("===================")
                        #print("entry at close:",datetime.fromtimestamp(entry["epoch"]), "entry at open:",datetime.fromtimestamp(forming_candle["epoch"]))
                        #print("==================")
                        
                        forming_candle["close"] = entry["quote"]
                        last_complete_index = tick_data.index(entry)
                        del forming_candle["count"]
                        market_data[symbol][f"candles_{tf}"].append(forming_candle)
                        #dsp.flux(forming_candle)
                        #print(market_data[symbol][f"candles_{tf}"])
                        forming_candle ={}
                        current_bin = None                                    
         
            
            
            #print(candles)
            yield i,forming_candle, candles
        cpu_end = time.perf_counter()
        if dsp:
            pass
            #dsp.flux("cpu delay",f"{cpu_end -cpu_start} seconds")
        else:
            pass
            #print(f"cpu delay:{cpu_end -cpu_start} seconds")
        await asyncio.sleep(0.1)


async def run(symbol,market_data):
    async for i,forming_candle,candles in handle_candles(symbol,market_data):
        print(forming_candle["count"],"s")  
              
            
async def main():
    #global tick_data
    markets=[]
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