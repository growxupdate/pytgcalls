import asyncio
import logging
import os
from concurrent.futures import CancelledError
from concurrent.futures import ThreadPoolExecutor
from typing import Any

from ntgcalls import NTgCalls

from .chat_lock import ChatLock
from .environment import Environment
from .methods import Methods
from .mtproto import MtProtoClient
from .scaffold import Scaffold
from .statictypes import statictypes
from .types import Cache

py_logger = logging.getLogger('pytgcalls')


class PyTgCalls(Methods, Scaffold):
    WORKERS = min(32, (os.cpu_count() or 0) + 4)
    CACHE_DURATION = 60 * 60

    @statictypes
    def __init__(
        self,
        app: Any,
        workers: int = WORKERS,
        cache_duration: int = CACHE_DURATION,
    ):
        super().__init__()
        self._mtproto = app
        self._app = MtProtoClient(
            cache_duration,
            self._mtproto,
        )
        self._is_running = False
        self._env_checker = Environment(
            self._REQUIRED_PYROGRAM_VERSION,
            self._REQUIRED_TELETHON_VERSION,
            self._REQUIRED_HYDROGRAM_VERSION,
            self._app.package_name,
        )
        self._cache_user_peer = Cache()
        self._binding = NTgCalls()
        self.loop = None
        self.workers = workers
        self._binding_futures = set()
        self._chat_lock = ChatLock()
        self.executor = ThreadPoolExecutor(
            self.workers,
            thread_name_prefix='Handler',
        )

    def _submit_binding_task(self, coro, name: str = 'binding_callback'):
        if self.loop is None or self.loop.is_closed():
            coro.close()
            py_logger.warning(
                'Dropping %s because the event loop is closed',
                name,
            )
            return None

        future = asyncio.run_coroutine_threadsafe(coro, self.loop)
        self._binding_futures.add(future)

        def _done(done_future):
            self._binding_futures.discard(done_future)
            try:
                done_future.result()
            except CancelledError:
                pass
            except Exception:
                py_logger.exception('Unhandled exception in %s', name)

        future.add_done_callback(_done)
        return future

    @property
    def cache_user_peer(self) -> Cache:
        return self._cache_user_peer

    @property
    def mtproto_client(self):
        return self._mtproto
