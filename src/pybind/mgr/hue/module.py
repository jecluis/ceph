import requests
import json
import threading
import errno
from enum import Enum
from mgr_module import MgrModule
from . import hue
from . import config


class Module(MgrModule):

    CONFIG_KEY = 'hue_config'
    MODULE_VERSION = 1

    COMMANDS = [
        {
            'cmd': 'hue setup '
                   'name=bridge,type=CephString',
            'desc': 'Setup a Hue bridge. Requires passing a config file.',
            'perm': 'w'
        },
        {
            'cmd': 'hue create-user '
                   'name=pressed,type=CephChoices,'
                   'strings=--pressed,req=false',
            'desc': 'Creates a user in the Hue bridge. '
                    'Requires pressing the bridge\'s link button',
            'perm': 'w'
        },
        {
            'cmd': 'hue config get',
            'desc': 'Obtain current config',
            'perm': 'r'
        },
        {
            'cmd': 'hue config set',
            'desc': 'Set a config',
            'perm': 'w'
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
        self._bridges = {}

        # just for eye candy, to be used for those methods defined by
        # the MgrModule instead.
        self.mgr = self
#        self.log.debug('init hue, load config')
#        self.load_config()

    def handle_health_status(self, status):
        if status == 'HEALTH_OK':
            self.log.debug('set lights to okay')
        elif status == 'HEALTH_WARN':
            self.log.debug('set lights to warn')
        elif status == 'HEALTH_ERR':
            self.log.debug('set lights to err')
        else:
            self.log.debug('unknown health status: {}'.format(status))

    def notify(self, notify_type, notify_id):
        if notify_type != 'health':
            return
        health = json.loads(self.get('health')['json'])
        self.log.debug('Received health update: {}'.format(health))

        if 'status' in health:
            self.handle_health_status(health['status'])
        self._event.set()

    def _init_bridges(self):
        for name, cfg in self._config.items():
            addr = cfg.get_address()
            user = cfg.get_user()
            bridge = HueBridge(addr, user)
            self._bridges[name] = bridge

    def serve(self):

        self.log.debug('loading config from store')
        self.load_config()
        self._init_bridges()

        while True:
            self._event.wait(10)  # hardcode for now
            self._event.clear()
            if self._shutdown:
                break

    def shutdown(self):
        self._shutdown = True
        self._event.set()

    def save_config(self):
        cfg = {
            'version': self.MODULE_VERSION,
            'config': dict([
                (name, cfg.get_raw_config())
                for name, cfg in self._config.items()])
        }
        self.log.debug('saving config: {}'.format(json.dumps(cfg)))
        self.mgr.set_store(self.CONFIG_KEY, json.dumps(cfg))

    def load_config(self):
        self.log.debug('load config')

        raw_cfg = self.mgr.get_store(self.CONFIG_KEY, None)
        if raw_cfg is None:
            self.log.debug('load config: no saved config found')
            return

        self.log.debug('config loaded: {}'.format(raw_cfg))

        self._raw_config = json.loads(raw_cfg)
        self._config = {}
        if 'config' not in self._raw_config:
            self.log.debug('load config: config not present')
            return

        for name, val in self._raw_config['config'].items():
            self.log.debug('creating config for \'{}\' with {}'.format(
                name, val))
            c = config.Config.create(name, val)
            self._config[name] = c
            self.log.debug('loaded config for \'{}\''.format(name))

    def handle_cmd_setup(self, cmd, inbuf):
        self.log.debug('handle setup')

        if 'bridge' not in cmd:
            return (-errno.EINVAL, '',
                    'Command requires a bridge name to be provided.')
        bridge_name = cmd['bridge']
        if not (isinstance(bridge_name, str) or
                isinstance(bridge_name, unicode)) or \
           len(bridge_name) == 0:
            return (-errno.EINVAL, '',
                    'Command requires a non-empty string as bridge name.')

        if inbuf is None:
            return (-errno.EINVAL, '',
                    'Command requires a config to be provided.')

        if not (isinstance(inbuf, str) or isinstance(inbuf, unicode)):
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

        self._config[bridge_name] = cfg
        self.save_config()
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
