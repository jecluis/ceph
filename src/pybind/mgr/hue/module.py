import requests
import json
import threading
import errno
from enum import Enum
from mgr_module import MgrModule
from . import hue
from . import config
from .hue import HueBridge

class Module(MgrModule):

    CONFIG_KEY = 'hue_config'
    MODULE_VERSION = 1

    COMMANDS = [
        {
            'cmd': 'hue config assimilate',
            'desc': 'Assimilate a full configuration file',
            'perm': 'rw'
        },
        {
            'cmd': 'hue config get',
            'desc': 'Obtain current config',
            'perm': 'r'
        },
        {
            'cmd': 'hue config set',
            'desc': 'Set a config',
            'perm': 'rw'
        },
        {
            'cmd': 'hue bridge setup '
                   'name=bridge,type=CephString',
            'desc': 'Setup a Hue bridge. Requires passing a config file.',
            'perm': 'rw'
        },
        {
            'cmd': 'hue bridge create-user '
                   'name=bridge,type=CephString '
                   'name=pressed,type=CephChoices,'
                   'strings=--pressed,req=false',
            'desc': 'Creates a user in the Hue bridge. '
                    'Requires pressing the bridge\'s link button',
            'perm': 'rw'
        },
        {
            'cmd': 'hue bridge ls',
            'desc': 'List configured bridges',
            'perm': 'r'
        },
        {
            'cmd': 'hue bridge info '
                   'name=bridge_name,type=CephString',
            'desc': 'Obtain bridge informations',
            'perm': 'r'
        }
    ]

    OPTIONS = [
        {'name': 'bridge_addr', 'default': ''},
        {'name': 'bridge_user', 'default': ''},
        {'name': 'groups'}
    ]

    def __init__(self, *args, **kwargs):
        super(Module, self).__init__(*args, **kwargs)

        self._shutdown = False
        self._event = threading.Event()
        self._config = {}
        self._raw_config = {}

        self._notify_lock = threading.Lock()
        self._systems_nominal = False

        from . import logger
        logger.log.setLogger(self.log)

        # just for eye candy, to be used for those methods defined by
        # the MgrModule instead.
        self.mgr = self

    def _init_bridges(self):
        self.log.debug("_init_bridges: {} in config".format(
            len(self._config.get_bridges())))

        for br_name, bridge in self._config.get_bridges().items():
            self.log.debug("_init_bridges: init {}".format(bridge.get_name()))
            assert br_name == bridge.get_name()
            bridge.init()
            self.log.debug("_init_bridges: init bridge {} "
                           "at {} with user {}".format(
                               bridge.get_name(),
                               bridge.get_address(),
                               bridge.get_user()))

    def _save_config(self):
        cfg = {
            'version': self.MODULE_VERSION,
            'config': dict([
                (name, cfg.get_raw_config())
                for name, cfg in self._config.items()])
        }
        self.log.debug('saving config: {}'.format(json.dumps(cfg)))
        self.mgr.set_store(self.CONFIG_KEY, json.dumps(cfg))

    def _load_config(self):
        self.log.debug('load config')

        raw_cfg = self.mgr.get_store(self.CONFIG_KEY, None)
        if raw_cfg is None:
            self.log.debug('load config: no saved config found')
            return

        self.log.debug('config loaded: {}'.format(raw_cfg))

        self._raw_config = json.loads(raw_cfg)
        self._config = config.Config.assimilate(self._raw_config)
        self.log.debug("_load_config: loaded = {}".format(
            json.dumps(self._config.to_jsonish())))

    def serve(self):
        self.log.debug("serve: loading config from store")
        self._load_config()
        try:
            self.log.debug("serve: initializing bridges")
            self._init_bridges()
        except Exception as e:
            raise e

        self._systems_nominal = True

        while True:
            self._event.wait(10)  # hardcode for now
            self._event.clear()
            if self._shutdown:
                break

    def notify(self, notify_type, notify_id):
        if not self._systems_nominal:
            self.log.debug("notify: Systems not Nominal; abort!!")
            return

        if notify_type != 'health':
            return
        health = json.loads(self.get('health')['json'])
        self.log.debug('Received health update: {}'.format(health))

        if 'status' in health:
            with self._notify_lock:
                self.handle_health_status(health['status'])
        self._event.set()

    def get_bridge(self, br_name):
        assert self._config.has_bridge(br_name)
        return config.get_bridge(br_name)

    def handle_health_status(self, status):
        if status == 'HEALTH_OK':
            self.log.debug('set lights to okay')
        elif status == 'HEALTH_WARN':
            self.log.debug('set lights to warn')
        elif status == 'HEALTH_ERR':
            self.log.debug('set lights to err')
        else:
            self.log.debug('unknown health status: {}'.format(status))

        if not self._config.has_bridges():
            self.log.debug("health status: non-existent config, ignore.")
            return

        self.log.debug("health_status: get groups for "
                       "status = {}".format(status))
        health_groups = self._config.get_status_groups(status)
        self.log.debug("health_status: groups = {}".format(health_groups))

        for br_name, br_groups in health_groups.items():
            assert len(br_groups) > 0
            for group in br_groups:
                status_color = group.get_status_color(status)
                self.log.debug("health status: "
                               "bridge {}, group {}, color {}".format(
                                   br_name, group.get_name(),
                                   status_color))
                bridge = self.get_bridge(br_name)
                bridge.set_group_state(group, status_color)

    def _shutdown_groups(self):
        self.log.debug('_shutdown_groups: shutting down')

        for name, cfg in self._config.items():
            self.log.debug('_shutdown_groups: shutting down for {}'.format(
                name))

            groups = cfg.get_groups_names()
            if name not in self._bridges:
                self.log.debug('_shutdown: unable to find bridge {}'.format(
                    name))
                continue

            bridge = self._bridges[name]
            if not bridge:
                self.log.debug('_shutdown: bridge {} does not exist'.format(
                    name))
                continue

            for grp_name in groups:
                self.log.debug('_shutdown: bridge {}, group {}'.format(
                    name, grp_name))
                bridge.shutdown_group(grp_name)

    def shutdown(self):
        self._shutdown = True
        self._shutdown_groups()

        self._event.set()

    def handle_cmd_setup(self, cmd, inbuf):
        self.log.debug('handle setup')

        if 'bridge' not in cmd:
            return (-errno.EINVAL, '',
                    'Command requires a bridge name to be provided.')
        bridge_name = cmd['bridge']
        if not isinstance(bridge_name, str) or len(bridge_name) == 0:
            return (-errno.EINVAL, '',
                    'Command requires a non-empty string as bridge name.')

        if inbuf is None:
            return (-errno.EINVAL, '',
                    'Command requires a config to be provided.')

        if not isinstance(inbuf, str):
            return (-errno.EINVAL, '', 'Provided config is not a string')
        try:
            cfg = config.Config.create(bridge_name, inbuf)
        except config.MalformedConfig as e:
            return (-errno.EINVAL, '',
                    'Error assimilating config: {}'.format(e.message))

        if not cfg.has_address():
            return (-errno.EINVAL, '',
                    'Missing \'address\' in config')

        if not cfg.has_user():
            out_msg = 'Configuration does not specify a Hue Bridge user. ' \
                      'You may create one at a later time using ' \
                      '`ceph hue create-user {}`'.format(bridge_name)
        else:
            out_msg = 'Bridge \'{}\' has been set up'.format(bridge_name)

        if bridge_name in self._config:
            self.log.debug('replacing config for \'{}\''.format(bridge_name))
        self._config[bridge_name] = cfg
        self._save_config()
        self._load_config()
        self._init_bridges()
        return (0, out_msg, '')

    def handle_cmd_create_user(self, cmd):
        self.log.debug('handle create user')

    def handle_cmd_config_get(self, cmd):
        self.log.debug('handle config get')
        out_msg = json.dumps(self._raw_config)
        self.log.debug('config: {}'.format(self._raw_config))
        return (0, out_msg, '')

    def handle_cmd_config_set(self, cmd, inbuf):
        self.log.debug('handle config set')

    def handle_cmd_bridge(self, cmd, inbuf):
        if cmd['prefix'] == 'hue bridge ls':
            out_lst = []
            for name, bridge in self._bridges.items():
                out_lst.append({
                    'name': name,
                    'address': bridge.get_address(),
                    'user': bridge.get_user()
                    })
            return (0, json.dumps(out_lst), '')
        elif cmd['prefix'] == 'hue bridge info':
            out_lst = []
            for name, bridge in self._bridges.items():
                out_lst.append({
                    'name': name,
                    'groups': bridge.get_groups()
                    })
            return (0, json.dumps(out_lst), '')

    def handle_command(self, inbuf, cmd):
        self.log.info('handle_command( '
                      'inbuf(len={}, type={}), cmd="{}")'.format(
                          len(inbuf), type(inbuf), cmd))

        if cmd['prefix'] == 'hue setup':
            return self.handle_cmd_setup(cmd, inbuf)
        elif cmd['prefix'] == 'hue create-user':
            return self.handle_cmd_create_user(cmd)
        elif cmd['prefix'] == 'hue config get':
            return self.handle_cmd_config_get(cmd)
        elif cmd['prefix'] == 'hue config set':
            return self.handle_cmd_config_set(cmd, inbuf)
        elif cmd['prefix'].startswith('hue bridge'):
            return self.handle_cmd_bridge(cmd, inbuf)
        else:
            return (-errno.EINVAL,
                    '',
                    'Unknown command \'{}\''.format(cmd['prefix']))
