#!/usr/bin/env python

import time
import asyncio


condition = asyncio.Condition()


@asyncio.coroutine
def echo_server():
    yield from asyncio.start_server(handle_connection, 'localhost', 8000)


@asyncio.coroutine
def handle_connection(reader, writer):
    while True:
        data = yield from condition
        if not data:
            break
        writer.write(str(condition))


loop = asyncio.get_event_loop()
loop.run_until_complete(echo_server())
loop.run_forever()

time.sleep(5)
condition.notify_all()

