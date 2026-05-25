import asyncio

from ntgcalls import ConnectionError
from ntgcalls import ConnectionNotFound

from ...scaffold import Scaffold


class RequestBroadcastTimestamp(Scaffold):
    TIMESTAMP_TIMEOUT = 10
    SEND_TIMEOUT = 10
    QUEUE_TIMEOUT = 5

    async def _request_broadcast_timestamp(
        self,
        chat_id: int,
    ):
        semaphore = getattr(self, '_broadcast_semaphore', None)
        acquired = False
        # noinspection PyBroadException
        try:
            if semaphore is not None:
                await asyncio.wait_for(
                    semaphore.acquire(),
                    timeout=self.QUEUE_TIMEOUT,
                )
                acquired = True
            time = await asyncio.wait_for(
                self._app.get_stream_timestamp(
                    chat_id,
                ),
                timeout=self.TIMESTAMP_TIMEOUT,
            )
        except Exception:
            time = 0
        finally:
            if acquired:
                semaphore.release()
        try:
            await asyncio.wait_for(
                self._binding.send_broadcast_timestamp(
                    chat_id,
                    time,
                ),
                timeout=self.SEND_TIMEOUT,
            )
        except (ConnectionError, ConnectionNotFound):
            pass
