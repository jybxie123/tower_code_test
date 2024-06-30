import asyncio
import websockets
import threading
import numpy as np
import pandas as pd
import pickle
import posix_ipc
import mmap
from config import COLUMNS, ROWS, SEMAPHORE_READ, SEMAPHORE_WRITE, SHARED_MEMORY_NAME, WS_PORT, WS_HOST

# server mode
client = None
message_queue = asyncio.Queue()
async def _handler(websocket, path):
    client = websocket
    try:
        while True:
            await asyncio.sleep(1)  # Keep the connection alive
    finally:
        client = None

async def _send_message(msg):
    if client:
        await client.send(msg)

async def _start_server(uri, port):
    async with websockets.serve(_handler, uri, port):
        await asyncio.Future() 

async def _recv_message(websocket, uri):
    async with websockets.connect(uri) as websocket:
        async for message in websocket:
            await message_queue.put(message)


class WebSocketDataTransfer:
    def __init__(self, host=WS_HOST, port=WS_PORT):
        self.server_host = host
        self.port = port
        self.client_uri = f"ws://{host}:{port}"

    def init_mtrx(self):
        threading.Thread(target=_start_server, daemon=True).start()
    
    def _start_server(self):
        asyncio.run(_start_server(self.server_host, self.port))

    def send_message(self, data): # data is df
        message = data.to_numpy().tobytes()
        asyncio.run_coroutine_threadsafe(_send_message(message), self.loop)

    def final_mtrx(self):
        if client:
            asyncio.run(client.close())

    def init_corr(self):
        threading.Thread(target=_recv_message, daemon=True).start()

    def receive_message(self):
        message = message_queue.get(block=True)
        return message


class SharedMemoryTransfer():
    def __init__(self, shm_name=SHARED_MEMORY_NAME, semaphore_write_name=SEMAPHORE_WRITE, semaphore_read_name=SEMAPHORE_READ,  shm_size=ROWS*COLUMNS*8):
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

    def corr_init(self):
        self.corr_memory = posix_ipc.SharedMemory(self.shm_name)
        self.corr_semaphore_write = posix_ipc.Semaphore(self.sema_write_name, posix_ipc.O_CREAT)
        self.corr_semaphore_read = posix_ipc.Semaphore(self.sema_read_name, posix_ipc.O_CREAT)
        self.corr_mem_map = mmap.mmap(self.corr_memory.fd, self.corr_memory.size)

    def receive_message(self):
        self.corr_semaphore_read.acquire()
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

