import asyncio
import logging
from typing import Any
from typing import Callable
from typing import NamedTuple
from typing import Optional

py_logger = logging.getLogger('pytgcalls')


class Callback(NamedTuple):
    func: Callable
    filters: Optional[Any]


class HandlersHolder:
    def __init__(self):
        self._callbacks = []

    async def _propagate(
        self,
        update,
        client=None,
    ):
        tasks = []
        for callback in list(self._callbacks):
            try:
                if client:
                    if callback.filters and not await callback.filters(
                        client,
                        update,
                    ):
                        continue
                    tasks.append(callback.func(client, update))
                else:
                    tasks.append(callback.func(update))
            except Exception:
                py_logger.exception('Error while preparing an update handler')

        if not tasks:
            return

        results = await asyncio.gather(*tasks, return_exceptions=True)
        for result in results:
            if isinstance(result, BaseException):
                if isinstance(result, asyncio.CancelledError):
                    continue
                py_logger.error(
                    'Unhandled exception in an update handler',
                    exc_info=(type(result), result, result.__traceback__),
                )

    def add_handler(
        self,
        func: Callable,
        filters=None,
    ) -> Callable:
        self._callbacks.append(Callback(func, filters))
        return func

    def remove_handler(
        self,
        func: Callable,
    ):
        self._callbacks = [x for x in self._callbacks if x.func != func]
