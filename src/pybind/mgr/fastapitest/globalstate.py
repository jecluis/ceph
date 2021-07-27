import asyncio
from mgr_module import MgrModule


class GlobalState:

    _counter: int = 0
    _running: bool = False
    _mgr: MgrModule

    def __init__(self, mgr: MgrModule):
        self._mgr = mgr
        pass

    def inc(self) -> int:
        self._counter += 1
        return self._counter

    @property
    def counter(self) -> int:
        return self._counter

    async def _tick(self) -> None:
        while self._running:
            self.inc()
            await asyncio.sleep(1.0)

    async def start(self) -> None:
        self._running = True
        self._task = asyncio.create_task(self._tick())

    async def shutdown(self) -> None:
        self._running = False
        await self._task

    @property
    def mgr(self) -> MgrModule:
        return self._mgr
