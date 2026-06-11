import json
import asyncio
import threading
import random
from websockets.asyncio.client import connect


class WShandler:
    def __init__(self):
        self.loop = asyncio.new_event_loop()
        self.loop_thread = threading.Thread(
            target=self._run_loop,
            name="ws_thread",
            daemon=True
        )

        self.queues = {}
        self.tasks = {}
        self.ws_tasks = {}
        self.shutdown_flag = False

        self.loop_thread.start()

    # ---------------- LOOP CORE ----------------

    def _run_loop(self):
        asyncio.set_event_loop(self.loop)
        self.loop.run_forever()

    def stop(self):
        """Graceful shutdown"""
        self.shutdown_flag = True

        def _stop():
            for t in self.tasks.values():
                t.cancel()
            for t in self.ws_tasks.values():
                t.cancel()
            self.loop.stop()

        self.loop.call_soon_threadsafe(_stop)
        self.loop_thread.join(timeout=5)

    # ---------------- PUBLIC API ----------------

    def start_feed(self, url, platform, data_type):
        key = f"{platform}_{data_type}"

        if key in self.ws_tasks:
            return

        fut = asyncio.run_coroutine_threadsafe(
            self._ws_supervisor(url, platform, data_type),
            self.loop
        )

        self.ws_tasks[key] = fut

    # ---------------- SUPERVISION LAYER ----------------

    async def _ws_supervisor(self, url, platform, data_type):
        key = f"{platform}_{data_type}"

        backoff = 1

        while not self.shutdown_flag:
            try:
                await self._connect_to_ws(url, platform, data_type)
                backoff = 1  # reset after success

            except asyncio.CancelledError:
                break

            except Exception as e:
                print(f"[{key}] error: {e}")

                await asyncio.sleep(backoff + random.random())
                backoff = min(backoff * 2, 30)

    # ---------------- CONNECTION LAYER ----------------

    async def _connect_to_ws(self, url, platform, data_type):
        key = f"{platform}_{data_type}"
        queue_key = f"{key}_queue"

        if queue_key not in self.queues:
            self.queues[queue_key] = asyncio.Queue(maxsize=20_000)

        queue = self.queues[queue_key]

        analyzer_key = f"{key}_analyzer"

        if analyzer_key not in self.tasks:
            self.tasks[analyzer_key] = asyncio.create_task(
                self._analyze_feed(platform, data_type)
            )

        print(f"[{key}] connecting...")

        async with connect(url) as ws:
            print(f"[{key}] connected")

            while not self.shutdown_flag:
                try:
                    raw = await ws.recv()
                    msg = json.loads(raw)

                    await queue.put(msg)

                except json.JSONDecodeError:
                    continue

                except Exception:
                    raise  # triggers reconnect supervisor

    # ---------------- CONSUMER LAYER ----------------

    async def _analyze_feed(self, platform, data_type):
        key = f"{platform}_{data_type}_queue"
        queue = self.queues[key]

        while not self.shutdown_flag:
            try:
                stream = await queue.get()
                await self._process(stream, platform, data_type)

            except asyncio.CancelledError:
                break

            except Exception as e:
                print(f"[consumer error {platform}] {e}")

    async def _process(self, msg, platform, data_type):
        print(platform, data_type, msg)