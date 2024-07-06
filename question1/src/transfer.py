import asyncio
import websockets
import threading
import numpy as np
import pandas as pd
import posix_ipc
import mmap
import datetime
import time
from config import COLUMNS, ROWS, SEMAPHORE_READ, SEMAPHORE_WRITE, \
    SHARED_MEMORY_NAME, WS_PORT, WS_HOST, TIMEOUT, SHARED_MEMORY, WEBSOCKET_MAX_SIZE, logging

class WebSocketDataTransfer:
    def __init__(self, host=WS_HOST, port=WS_PORT):
        self.server_host = host
        self.port = port
        self.client_uri = f"ws://{host}:{port}"
        self.client = None
        self.message_queue = asyncio.Queue()
        self.loop = asyncio.new_event_loop()
        self.recv_loop = asyncio.new_event_loop()

    async def _handler(self, websocket, path):
        self.client = websocket
        try:
            while True:
                await asyncio.sleep(1)  # Keep the connection alive
        finally:
            self.client = None

    async def _send_message(self, msg):
        if self.client:
            transfer_start_time = datetime.now()
            await self.client.send(msg)
            transfer_end_time = datetime.now()
            logging.info(f'===Program 1=== : send data to calulator, using time : {(transfer_end_time - transfer_start_time).total_seconds()}')

    async def _start_server(self):
        async with websockets.serve(
            self._handler, self.server_host, self.port, max_size=WEBSOCKET_MAX_SIZE
            ):
            await asyncio.Future()  # Run forever

    def start_server(self):
        asyncio.set_event_loop(self.loop)
        self.loop.run_until_complete(self._start_server())
        self.loop.run_forever()

    def mtrx_init(self):
        threading.Thread(target=self.start_server, daemon=True).start()

    def send_message(self, data):
        message = data.to_numpy().tobytes()
        print(f"Message size: {len(message)} bytes")
        asyncio.run_coroutine_threadsafe(self._send_message(message), self.loop)

    def final_mtrx(self):
        if self.client:
            asyncio.run_coroutine_threadsafe(self.client.close(), self.loop)

    async def _recv_message(self):
        try:
            async with websockets.connect(self.client_uri, max_size=WEBSOCKET_MAX_SIZE) as websocket:
                async for message in websocket:
                    await self.message_queue.put(message)
        except websockets.ConnectionClosedError as e:
            logging.info(f"===Program 2=== : Connection closed: {e}")

    def _run_event_loop(self):
        asyncio.set_event_loop(self.recv_loop)
        self.recv_loop.run_until_complete(self._recv_message())
        self.recv_loop.run_forever()

    def corr_init(self):
        threading.Thread(target=self._run_event_loop, daemon=True).start()

    def receive_message(self, timeout=TIMEOUT):
        try:
            message = asyncio.run_coroutine_threadsafe(self.message_queue.get(), self.recv_loop).result(timeout=timeout)
            array = np.frombuffer(message, dtype=np.float64)
            array = array.reshape((ROWS, COLUMNS))
            return pd.DataFrame(array, columns=[f'Column{i}' for i in range(COLUMNS)])
        except asyncio.TimeoutError:
            logging.info("===Program 2=== : Message receiving timed out.")
            raise TimeoutError("Message received timed out.")


class SharedMemoryTransfer():
    def __init__(self, shm_name=SHARED_MEMORY_NAME, semaphore_write_name=SEMAPHORE_WRITE, 
                 semaphore_read_name=SEMAPHORE_READ,  shm_size=SHARED_MEMORY):
        self.shm_name = shm_name
        self.shm_size = shm_size
        self.sema_write_name = semaphore_write_name
        self.sema_read_name = semaphore_read_name

        self.mtrx_memory = None
        self.mtrx_semaphore_write = None
        self.mtrx_semaphore_read = None
        self.mtrx_mem_map = None
        
        self.corr_memory = None
        self.corr_semaphore_write = None
        self.corr_semaphore_read = None
        self.corr_mem_map = None

    def mtrx_init(self):
        self.mtrx_memory = posix_ipc.SharedMemory(self.shm_name, posix_ipc.O_CREAT, size=self.shm_size) 
        self.mtrx_semaphore_write = posix_ipc.Semaphore(self.sema_write_name, posix_ipc.O_CREAT, initial_value=1)
        self.mtrx_semaphore_read = posix_ipc.Semaphore(self.sema_read_name, posix_ipc.O_CREAT, initial_value=0)
        self.mtrx_mem_map = mmap.mmap(self.mtrx_memory.fd, self.mtrx_memory.size)

    def corr_init(self):
        self.corr_memory = posix_ipc.SharedMemory(self.shm_name)
        self.corr_semaphore_write = posix_ipc.Semaphore(self.sema_write_name, posix_ipc.O_CREAT)
        self.corr_semaphore_read = posix_ipc.Semaphore(self.sema_read_name, posix_ipc.O_CREAT)
        self.corr_mem_map = mmap.mmap(self.corr_memory.fd, self.corr_memory.size)

    def send_message(self, data):
        threading.Thread(target=self._send_message_thread, args=(data,), daemon=True).start()

    def final_mtrx(self):
        self.mtrx_mem_map.close()
        self.mtrx_memory.close_fd()
        self.mtrx_memory.unlink()
        self.mtrx_semaphore_read.unlink()
        self.mtrx_semaphore_write.unlink()

    def _send_message_thread(self, data):
        self.mtrx_semaphore_write.acquire()
        self.mtrx_mem_map.seek(0)
        self.mtrx_mem_map.write(data.to_numpy().tobytes())
        self.mtrx_semaphore_read.release()

    def receive_message(self, timeout=TIMEOUT):
        if self.corr_semaphore_read.acquire(timeout=timeout):
            logging.info(f"===Program 2=== : Message receiving timed out.")
            raise TimeoutError("Message received timed out.")
        self.corr_mem_map.seek(0)
        corr_matrix = np.frombuffer(self.corr_mem_map.read(self.shm_size), dtype=np.float64).reshape((ROWS, COLUMNS))
        self.corr_semaphore_write.release()
        return pd.DataFrame(corr_matrix, columns=[f"Column{i}" for i in range(COLUMNS)])


TRANSFER_DICT = {
    'websocket': WebSocketDataTransfer,
    'shared_memory': SharedMemoryTransfer,
}

class TransferFactory:
    _instance = None

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super(TransferFactory, cls).__new__(cls)
        return cls._instance
    
    @staticmethod
    def create_transfer_method(transfer_type, *args, **kwargs):
        if transfer_type in TRANSFER_DICT:
            return TRANSFER_DICT[transfer_type]( **kwargs)
        else:
            raise ValueError(f"Unsupported transfer method: {transfer_type}")


if __name__ == "__main__":
    trans = WebSocketDataTransfer()
    print('start mtrx transfer')
    trans.mtrx_init()
    print('start mtrx')
    trans.corr_init()
    print('start corr')
    while not trans.client:
        time.sleep(0.1)
    print('ready')
    matrix = np.random.rand(ROWS, COLUMNS)
    df = pd.DataFrame(matrix, columns=[f'Column{i}' for i in range(COLUMNS)])
    trans.send_message(df)
    
    new_df = trans.receive_message()
    print(new_df)
    print(new_df.head(5))
    trans.final_mtrx()
