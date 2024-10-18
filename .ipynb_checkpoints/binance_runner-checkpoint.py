import aiohttp
import time
import asyncio


async def run_binance_ws(time_seconds, q, i):
    timeout = int(time_seconds * 10 ** 9)
    start = time.time_ns()
    url = "wss://fstream.binance.com/ws/btcusdt@bookTicker"
    res = []
    print(f'process {i} started')
    async with aiohttp.ClientSession() as session:
        async with session.ws_connect(url, verify_ssl=False) as ws:
            async for msg in ws:
                ts = time.time_ns()
                res.append({'msg': msg, 'at': ts})
                if ts - start >= timeout:
                    q.put(res)
                    print(f'process {i} finished')
                    return

def run(time_seconds, q, i):
    asyncio.run(run_binance_ws(time_seconds, q, i))