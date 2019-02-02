import json
from . import logger
from .logger import log
from .hue import HueColors
from .bridge_config import BridgeConfig, MalformedBridgeConfig

class MalformedConfig(Exception):
    pass

class Config:

    def __init__(self):
        self._format = None
        self._version = 0
        self._bridges = []
        self._enabled_bridges = []

    @classmethod
    def setup_bridge(cls, name, cfg):
        c = cls()
        c._setup_bridge(name, cfg)
        return c

    @classmethod
    def assimilate(self, cfg):
        c = cls()
        c._assimilate(cfg)
        return c

    def _assimilate(self, cfg):
        log.debug('config assimilate: {} = {}'.format(name, cfg))

        if not isinstance(name, str) or len(name) == 0:
            raise MalformedConfig("expecting a non-zero string as name")

        cfg = self._get_from_json(cfg)

        version = int(cfg['version']) if 'version' in cfg else None
        config_format = cfg['format'] if 'format' in cfg else None
        bridge_lst = cfg['bridges'] if 'bridges' in cfg else None

        for bridge in bridge_lst:
            try:
                brg = BridgeConfig.create(bridge)
            except MalformedBridgeConfig(e):
                raise MalformedConfig(e)
            self._bridges[brg.get_name()] = brg

        if version is None:
            self._version = 1

    def _get_from_json(self, cfg):
        if isinstance(cfg, str):
            try:
                cfg = json.loads(cfg)
            except SyntaxError as e:
                raise MalformedConfig("malformed json config")
        elif not isinstance(cfg, dict):
            raise MalformedConfig("expected a 'str' or a 'dict'")
        return cfg

    def _setup_bridge(self, name, cfg):
        log.debug("setup bridge {}, config = {}".format(name, cfg))

        if not isinstance(name, str) or len(name) == 0:
            raise MalformedConfig("expecting a non-zero string as name")
        cfg = self._get_from_json(cfg)

        try:
            bridge = BridgeConfig.create(name, cfg)
        except MalformedBridgeConfig as e:
            raise MalformedConfig(e)
        self._bridges.append(bridge)

    def to_jsonish(self):
        bridge_lst = []
        for bridge in self._bridges:
            bridge_lst.append(bridge.to_jsonish())
        d = {
                'format': self._format,
                'version': self._version,
                'enabled': self._enabled,
                'bridges': bridge_lst
            }
        return d

    def is_bridge_enabled(self, name):
        return name in self._enabled_bridges

    def bridge_enable(self, name):
        if not self._bridge_exists(name) or \
           not self.is_bridge_enabled(name):
            return False
        self._enabled_bridges.append(name)
        self._bridges[name].enable()
        return True

    def bridge_disable(self, name):
        if name not in self._enabled_bridges:
            return True
        self._bridges[name].disable()
        self._enabled_bridges.remove(name)
        return True

    def get_raw_config(self):
        return json.dumps(self.to_jsonish())

    def get_status_groups(self, status_str):
        groups = {}
        for br_name in self._enabled_bridges:
            br_groups = self._bridges[br_name].get_status_groups(status_str)
            if br_groups is None:
                continue
            groups[br_name] = br_groups
        return groups

