from .logger import log
from .hue import HueColors, HueBridge, HueError


class MalformedBridgeConfig(Exception):
    pass

class MalformedBridgeGroup(MalformedBridgeConfig):
    pass

def UnknownColor(Exception):
    pass
def UnknownColorType(Exception):
    pass
def MissingColor(Exception):
    pass

class BridgeConfig:

    def __init__(self):
        self._name = None
        self._address = None
        self._user = None
        self._groups = []
        self._is_enabled = False

    @classmethod
    def create(cls, cfg):
        log.debug("bridge.create: config = {}".format(cfg))
        if 'name' not in cfg:
            raise MalformedBridgeConfig("create: missing bridge name")
        bridge_name = cfg['name']
        log.debug("bridge.create: create bridge {}".format(bridge_name))
        # technically, I think, we could make the decision here on whether
        # this is a hue bridge or something else, and callout the appropriate
        # subcall to handle everything. As it is, we're going with just the
        # 'Bridge' class declared later in this file
        c = Bridge()
        # let any exceptions just leak to the caller
        c.assimilate(bridge_name, cfg)
        return c

    def _assimilate_groups(self, groups_cfg):
        log.debug("bridge.assimilate_groups: {}".format(groups_cfg))
        if not isinstance(groups_cfg, list):
            raise MalformedBridgeConfig("expected list of groups")

        # nothing to do, move along
        if len(groups_cfg) == 0:
            log.debug("bridge.assimilate_groups: no groups")
            return

        self._groups = []
        for group in groups_cfg:
            try:
                status_group = BridgeStatusGroup(group)
            except MalformedBridgeGroup as e:
                raise MalformedBridgeConfig(e)
            self._groups.append(status_group)
        log.debug("bridge.assimilate_groups: groups assimilated")

    def assimilate(self, name, cfg):
        self._name = name

        if 'user' in cfg:
            self._user = cfg['user']
            assert isinstance(self._user, str)
        if 'address' in cfg:
            self._address = cfg['address']
            assert isinstance(self._address, str)
        if 'groups' in cfg:
            self._assimilate_groups(cfg['groups'])
        log.debug("bridge.assimilate: assimilated")

    def enable(self):
        self._is_enabled = True

    def disable(self):
        self._is_enabled = False

    def is_enabled(self):
        return self._is_enabled;

    def set_user(self, val):
        assert isinstance(val, str)
        self._user = val

    def set_address(self, val):
        assert isinstance(val, str)
        self._address = val

    def has_address(self):
        if self._address is not None:
            assert isinstance(self._address, str)
            return len(self._address) > 0
        return False

    def has_user(self):
        if self._user is not None:
            assert isinstance(self._user, str)
            return len(self._user) > 0
        return False

    def get_address(self):
        return self._address

    def get_user(self):
        return self._user

    def get_name(self):
        return self._name

    def _get_groups_jsonish(self):
        groups_lst = []
        for group in self._groups:
            groups_lst.append(group.to_jsonish())
        return groups_lst

    def to_jsonish(self):
        d = {
                'name': self._name,
                'address': self._address,
                'user': self._user,
                'groups': self._get_groups_jsonish()
            }
        return d

    def get_groups(self):
        return self._groups

    def get_status_groups(self, status_str):
        groups_lst = []
        for group in self._groups:
            if not group.is_status_group() or \
               not group.handles_status(status_str):
                continue
            groups_lst.append(group)
        return groups_lst

    def is_okay_to_enable(self):
        return self.has_address() and self.has_user()

class BridgeGroup:

    def __init__(self):
        self._name = None

    def is_status_group(self):
        raise NotImplementedError

    def set_name(self, name):
        self._name = name

    def get_name(self):
        return self._name

class BridgeStatusGroup(BridgeGroup):

    def __init__(self, group):
        super().__init__()
        self._status_groups = {}

        log.debug("bridge_sgroup: init cfg = {}".format(group))

        if 'name' not in group:
            raise MalformedBridgeGroup("group requires a name")
        self.set_name(group['name'])

        if 'status' not in group:
            raise MalformedBridgeGroup(
                    "group {} doesn't contain status desc".format(self._name))

        if not isinstance(group['status'], dict):
            raise MalformedBridgeGroup(
                    "group {} status groups are not a dictionary".format(
                        self._name))

        if len(group['status']) == 0:
            raise MalformedBridgeGroup(
                    "group {} status groups not defined".format(self._name))

        self._create_status_groups(group['status'])

    def __repr__(self):
        return 'StatusGroup(name = "{}", {} status entries)'.format(
            self._name, len(self._status_groups))

    def is_status_group(self):
        return True

    def _create_status_groups(self, sgroup):
        for status, color_data in sgroup.items():
            try:
                color_info = BridgeStatusColor(color_data)
            except MissingColor:
                raise MalformedBridgeGroup(
                        "color not specified for status {}, group {}".format(
                            status, self._name))
            except UnknownColor:
                raise MalformedBridgeGroup(
                        "unknown color {} for status {}, group {}".format(
                            color_info['color'], status, self._name))
            except UnknownColorType:
                raise MalformedBridgeGroup(
                        "unknown color type {} for status {}, "
                        "group {}".format(color_info['type'], status,
                            self._name))
            self._status_groups[status] = color_info

    def set_hue_group(self, grp):
        logging.debug("setting hue group {}".format(grp))
        self.hue_group = grp

    def handles_status(self, status_str):
        return status_str in self._status_groups

    def get_status_color(self, status_str):
        return self._status_groups[status_str]

    def to_jsonish(self):
        status_groups = {}
        for status, color_info in self._status_groups.items():
            status_groups[status] = color_info.to_jsonish()
        d = {'name': self._name, 'status': status_groups}
        return d

class BridgeStatusColor:
    TYPE_SOLID = 1
    TYPE_ALERT = 2

    type_names = {"solid": TYPE_SOLID, "alert": TYPE_ALERT}

    def __init__(self, color_info):
        if 'color' not in color_info:
            raise MissingColor()
        self._color_name = color_info['color']
        try:
            self._color = HueColors.by_name(self._color_name)
        except KeyError as e:
            log.error("unknown color '{}'".format(self._color_name))
            raise UnknownColor()

        if 'type' not in color_info:
            self._type = self.TYPE_SOLID
        else:
            try:
                self._type = self.type_names[color_info['type']]
            except KeyError as e:
                raise UnknownColorType()

    def __repr__(self):
        return 'StatusColor(color = {}, type = {})'.format(
            self._color, self.type_to_str(self._type))

    def to_jsonish(self):
        d = {
                'color': self._color_name,
                'type': self.type_to_str(self._type)
            }
        return d

    @classmethod
    def type_to_str(cls, type):
        for k, v in cls.type_names.items():
            if v == type:
                return k
        return "unknown"

    def is_alert(self):
        return self._type == self.TYPE_ALERT

    def get_color(self):
        return self._color


class Bridge(BridgeConfig):

    def __init__(self):
        super().__init__()
        self._bridge = None

    def init(self):
        log.debug("bridge: initialize...")

        if self._address is None or self._user is None:
            log.info("bridge.init: unable to initialize due to missing info: "
                     "address = {}, user = {}".format(
                         self._address,
                         self._user))
            return False

        log.debug("bridge.init: address = {}, user = {}".format(
            self._address, self._user))
        self._bridge = HueBridge(self._address, self._user)
        try:
            self._bridge.self_test()
        except HueError as e:
            log.info("bridge.init: error self-testing hue: {}".format(e))
            return False
        return True

    def disable(self):
        log.debug("bridge.disable: disabling bridge {}".format(self._name))
        super().disable()
        self._shutdown()

    def set_group_state(self, group, status_color):
        group_name = group.get_name()
        return self._bridge.set_group_state(group_name, status_color)

    def _shutdown(self):
        log.debug("bridge._shutdown: shutdown bridge {}".format(self._name))
        status_groups = self.get_groups()
        for group in status_groups:
            gr_name = group.get_name()
            log.debug("bridge._shutdown: shutdown group {}".format(gr_name))
            self._bridge.shutdown_group(gr_name)

