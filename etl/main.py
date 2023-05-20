import json
import time

from anyio import create_memory_object_stream, open_file, create_task_group, run, sleep
from anyio.streams.memory import MemoryObjectSendStream, MemoryObjectReceiveStream


async def extract(send_stream: MemoryObjectSendStream):
    async with await open_file('etl/data/departments.json') as f:
        async with send_stream:
            await sleep(3)
            while line := await f.readline():
                await send_stream.send(line)


async def transform(receive_stream: MemoryObjectReceiveStream, send_stream: MemoryObjectSendStream):
    async with receive_stream, send_stream:
        async for record in receive_stream:
            str_data = json.dumps(record, ensure_ascii=False)
            await send_stream.send(str_data.upper())


async def load(receive_stream: MemoryObjectReceiveStream):
    async with await open_file('etl/data/cap_departments.txt', 'w', encoding='utf8') as f:
        await sleep(2)
        async for record in receive_stream:
            await f.write(record)


async def main():
    start = time.time()
    send_transform_stream, receive_transform_stream = create_memory_object_stream()
    send_load_stream, receive_load_stream = create_memory_object_stream()

    async with create_task_group() as tg:
        tg.start_soon(extract, send_transform_stream)
        tg.start_soon(transform, receive_transform_stream, send_load_stream)
        tg.start_soon(load, receive_load_stream)
    print(time.time() - start)

if __name__ == '__main__':
    run(main)

