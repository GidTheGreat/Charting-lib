"""
This module will handle all websocket functionality.It takes arguments:platform i.e. binance,okx, data_type i.e. futures or spot, url
"""
import json
import asyncio
import threading
from collections import defaultdict
from websockets.asyncio.client import connect

class WShandler:
    """The websocket handler"""
    def __init__(self):
        self.loop = asyncio.new_event_loop()
        self.loop_thread = threading.Thread(target=self.loop.run_forever,
        name="ws_thread")
        self.loop_thread.start()
        #global reference watch out for cross thread
        #access
        self.queues = {}
    
    def start_feed(self, url,platform, data_type):
        """Schedule ws connection on
        event loop already running thread safely """
        asyncio.run_coroutine_threadsafe(
        self.connect_to_ws(url, 
        platform,data_type), self.loop)
    
    async def analyze_feed(self,platform, data_type):
        """Access the relevant queue get data and
        analyze"""
        while True:
            stream = await self.queues[
            f"{platform}_{data_type}_queue"
            ].get()
            print(platform,data_type,stream)
           
    async def connect_to_ws(self,url,platform,data_type):
        """Establish websocket connection,
        create queue, read websocket feed push to queue,
        start feed analyzer"""
        print(f"connecting to {platform} "
              f"for {data_type} stream")
              
        async with connect(url) as ws:
            print(f"connecting to {platform} "
                  f"for {data_type} stream")
            key = f"{platform}_{data_type}_queue"
            if key in self.queues:
                raise ValueError("feed already exists")
            self.queues[key] = asyncio.Queue(maxsize=20_000)
            #we are already in event loop hence
            asyncio.create_task(
            self.analyze_feed(
            platform,data_type))
            queue = self.queues[key]      
            while True:
                msg = json.loads(await ws.recv())
                await queue.put(msg)