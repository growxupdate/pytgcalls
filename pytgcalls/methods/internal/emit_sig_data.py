import asyncio
import logging

from ...scaffold import Scaffold

py_logger = logging.getLogger('pytgcalls')


class EmitSigData(Scaffold):
    SIGNALING_TIMEOUT = 10

    async def _emit_sig_data(self, chat_id: int, data: bytes):
        try:
            await asyncio.wait_for(
                self._app.send_signaling(
                    chat_id,
                    data,
                ),
                timeout=self.SIGNALING_TIMEOUT,
            )
        except Exception as e:
            py_logger.debug('SendSignalingData failed: %s', e)
