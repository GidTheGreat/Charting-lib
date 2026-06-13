import asyncio
from collections import defaultdict
from dataclasses import dataclass, asdict
import pandas as pd
from datetime import datetime

# -----------------------------
# Candle structure
# -----------------------------
@dataclass
class Candle:
    bucket: int
    open: float
    high: float
    low: float
    close: float
    open_epoch:int
    close_epoch:int
    raw_profile:dict
    binned_profile:list


# -----------------------------
# Timeframe engine
# -----------------------------
class CandleEngine:
    def __init__(self, timeframes):
        """
        timeframes: dict like
        {
            "1min": 60,
            "5min": 300,
            "15min": 900
        }
        """
        self.timeframes = timeframes

        # active candles per timeframe
        self.active = defaultdict(lambda: None)

        # completed candles storage
        self.completed = defaultdict(list)
        self.dt = datetime.fromtimestamp
        self.profile = defaultdict(lambda: defaultdict(lambda: {"buy": 0.0, "sell": 0.0}))

    # -------------------------
    # Main tick processor
    # -------------------------
    def on_tick(self, epoch: int, price: float, qty: float, side: str):
        #print("new tick", datetime.fromtimestamp(epoch),epoch)
        for tf_name, tf_secs in self.timeframes.items():
            self._update(tf_name, tf_secs, epoch, price, qty, side)

    # -------------------------
    # Update single timeframe
    # -------------------------
    def _update(self, tf, tf_secs, epoch, price, qty, side):
        bucket = epoch // tf_secs
        self._add_volume(tf, price, qty, side)

        candle = self.active[tf]

        # ---------------------
        # first candle or new bucket
        # ---------------------
        if candle is None or candle.bucket != bucket:
            if candle is not None:
                bins = self._build_profile_bins(tf, candle, n_bins=10)
                candle.binned_profile = bins
                candle.raw_profile = self.profile[tf]
                self.completed[tf].append(asdict(candle))
                print(tf,"open:",self.dt(candle.open_epoch),"close:",self.dt(candle.close_epoch),candle)
                print("PROFILE BINS:")
                for b in bins:
                    print(b)
                print("=================/=====/=======")
                # reset profile for next candle
                self.profile[tf].clear()

            self.active[tf] = Candle(
                open_epoch=epoch,
                close_epoch=epoch,
                bucket=bucket,
                open=price,
                high=price,
                low=price,
                close=price,
                raw_profile=0,
                binned_profile=0
            )
            return

        # ---------------------
        # update existing candle
        # ---------------------
        candle.high = max(candle.high, price)
        candle.low = min(candle.low, price)
        candle.close = price
        candle.close_epoch = epoch

    def _add_volume(self, tf, price, qty, side):
        bin_ = self.profile[tf][price]
        if side == "buyer":
            bin_["buy"] += qty
        else:
            bin_["sell"] += qty
            
    def _build_profile_bins(self, tf, candle, n_bins=10):
        profile = self.profile[tf]
        if not profile:
            return []

        prices = list(profile.keys())

        low = min(prices)
        high = max(prices)

        if high == low:
            return [{
                "price": low,
                "buy": sum(v["buy"] for v in profile.values()),
                "sell": sum(v["sell"] for v in profile.values())
            }]

        step = (high - low) / n_bins

        bins = [
            {"buy": 0.0, "sell": 0.0, "low": low + i * step, "high": low + (i + 1) * step}
            for i in range(n_bins)
        ]

        for price, vol in profile.items():
            idx = int((price - low) / step)
            if idx == n_bins:  # edge case for max price
                idx -= 1
            bins[idx]["buy"] += vol["buy"]
            bins[idx]["sell"] += vol["sell"]

        return bins
    # -------------------------
    # Force flush (e.g. shutdown)
    # -------------------------
    def flush(self):
        for tf, candle in self.active.items():
            if candle:
                self.completed[tf].append(asdict(candle))
        self.active.clear()


# -----------------------------
# Async stream simulator / handler
# -----------------------------
async def handle_stream(symbol, market_data, engine: CandleEngine):
    """
    market_data[symbol]["tick_data"] = list of:
        {"epoch": int, "quote": float}
    """
    i = 0

    while True:
        ticks = market_data[symbol]["tick_data"]

        while i < len(ticks):
            tick = ticks[i]
            i += 1

            engine.on_tick(tick["epoch"], tick["quote"],tick["volume"],tick["aggressor"])

        await asyncio.sleep(0.05)
        
        
async def main():
    """Entry point
    """
    timeframes = {
            "1min": 60
        }
    engine = CandleEngine(timeframes)
    market_data = defaultdict(lambda:defaultdict(list))
    asyncio.create_task(handle_stream("R",market_data,engine))
    df = pd.read_csv("BTCUSDT_1780935750_1781111401.csv")
    lst = df.to_dict(orient="records")
    print(len(lst))
    lst = lst[0:3000]
    for i,item in enumerate(lst):
        market_data["R"]["tick_data"].append(item)
        #print("we are in main i =",i,datetime.fromtimestamp(item["epoch"]),item["quote"])
        #await asyncio.sleep(10)

   
if __name__ == "__main__":
    asyncio.run(main())
           