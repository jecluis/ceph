import asyncio
from typing import Any, Optional

import uvicorn
from mgr_module import CLIReadCommand, HandleCommandResult, MgrModule, Option
from threading import Event

from fastapi import FastAPI

from fastapitest.api import counter, health
from fastapitest.globalstate import GlobalState


class FastAPITest(MgrModule):

    MODULE_OPTIONS = []
    NATIVE_OPTIONS = []

    running: bool = False

    test_app: Optional[FastAPI] = None
    test_app_api: Optional[FastAPI] = None

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        pass

    async def _startup(self):
        self.log.info("Startup FastAPI")
        assert self.test_app_api
        gstate = GlobalState(self)
        self.test_app_api.state.gstate = gstate
        self.running = True
        await gstate.start()

    async def _shutdown(self):
        self.log.info("Shutdown FastAPI")
        assert self.test_app_api
        gstate: GlobalState = self.test_app_api.state.gstate
        await gstate.shutdown()

    def serve(self) -> None:
        self.log.info("Starting FastAPI test server")
        self.test_app = FastAPI(docs_url=None)
        self.test_app_api = FastAPI(title="Testing FastAPI in the ceph-mgr")

        self.test_app.add_event_handler("startup", self._startup)
        self.test_app.add_event_handler("shutdown", self._shutdown)

        self.test_app_api.include_router(counter.router)
        self.test_app_api.include_router(health.router)

        self.test_app.mount("/api", self.test_app_api, name="api")
        uvicorn.run(self.test_app, host="0.0.0.0", port=1337)

    def shutdown(self) -> None:
        self.log.info("Shutting down FastAPI test server")
        if not self.test_app:
            return
