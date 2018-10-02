import requests
import json
import threading
from enum import Enum
from mgr_module import MgrModule
from . import hue
from . import config


class Module(MgrModule):

    def __init__(self, *args, **kwargs):
        super(Module, self).__init__(*args, **kwargs)

        self._shutdown = False
        self._event = threading.Event()

    def notify(self, notify_type, notify_id):
        if notify_type != "health":
            return
        health = json.loads(self.get("health")["json"])
        self.log.debug("Received health update: {}".format(health))
        self._event.set()

    def serve(self):
        while True:
            self._event.wait(10)  # hardcode for now
            self._event.clear()
            if self._shutdown:
                break

    def shutdown(self):
        self._shutdown = True
        self._event.set()
