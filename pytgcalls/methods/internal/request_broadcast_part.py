import asyncio

from ntgcalls import ConnectionError
from ntgcalls import ConnectionNotFound
from ntgcalls import MediaSegmentStatus
from ntgcalls import SegmentPartRequest

from ...scaffold import Scaffold


class RequestBroadcastPart(Scaffold):
    DOWNLOAD_TIMEOUT = 20
    SEND_TIMEOUT = 10
    QUEUE_TIMEOUT = 5

    async def _request_broadcast_part(
        self,
        chat_id: int,
        part_request: SegmentPartRequest,
    ):
        part_status = MediaSegmentStatus.NOT_READY
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
            part = await asyncio.wait_for(
                self._app.download_stream(
                    chat_id,
                    part_request.timestamp,
                    part_request.limit,
                    part_request.channel_id
                    if part_request.channel_id > 0 else None,
                    part_request.quality,
                ),
                timeout=self.DOWNLOAD_TIMEOUT,
            )
            if part is not None:
                part_status = MediaSegmentStatus.SUCCESS
        except Exception:
            part = None
            part_status = MediaSegmentStatus.RESYNC_NEEDED
        finally:
            if acquired:
                semaphore.release()

        try:
            await asyncio.wait_for(
                self._binding.send_broadcast_part(
                    chat_id,
                    part_request.segment_id,
                    part_request.part_id,
                    part_status,
                    part_request.quality_update,
                    part,
                ),
                timeout=self.SEND_TIMEOUT,
            )
        except (ConnectionError, ConnectionNotFound):
            pass
