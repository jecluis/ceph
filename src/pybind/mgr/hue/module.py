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

        # just for eye candy, to be used for those methods defined by
        # the MgrModule instead.
        self.mgr = self

    def notify(self, notify_type, notify_id):
        if notify_type != 'health':
            return
        health = json.loads(self.get('health')['json'])
        self.log.debug('Received health update: {}'.format(health))
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

    def save_config(self):
        cfg = {
            'version': self.MODULE_VERSION,
            'config': dict([
                (name, cfg.get_raw_config())
                for name, cfg in self._config.items()])
        }
        self.mgr.set_store(self.CONFIG_KEY, json.dumps(cfg))

    def load_config(self):
        raw_cfg = self.mgr.load_store(self.CONFIG_KEY, None)
        if raw_cfg is None:
            self.log.debug('load config: no saved config found')
            return

        cfg = json.loads(raw_cfg)
        self._config = {}
        for name, val in cfg['config']:
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

    def handle_command(self, inbuf, cmd):
        self.log.debug('handle_command( '
                       'inbuf(len={}, type={}), cmd="{}")'.format(
                           len(inbuf), type(inbuf), cmd))

        if cmd['prefix'] == 'hue setup':
            return self.handle_cmd_setup(cmd, inbuf)
        elif cmd['prefix'] == 'hue create-user':
            return self.handle_cmd_create_user(cmd)
        else:
            return (-errno.EINVAL,
                    '',
                    'Unknown command \'{}\''.format(cmd['prefix']))
