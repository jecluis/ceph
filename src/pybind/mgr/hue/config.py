import json
import logger
from logger import log
from hue import HueColors


class MalformedConfig(Exception):
    pass


class StatusColor:
    TYPE_SOLID = 1
    TYPE_ALERT = 2

    type_names = {"solid": TYPE_SOLID, "alert": TYPE_ALERT}

    def __init__(self, name, type):
        if not name or not type:
            raise MalformedConfig("malformed color")
        try:
            self.color = HueColors.by_name(name)
        except KeyError as e:
            log.error("unknown color '{}'".format(name))
            raise MalformedConfig(e)

        if type.lower() not in self.type_names:
            self.type = self.TYPE_SOLID
        else:
            self.type = self.type_names[type]

    def __repr__(self):
        return 'StatusColor(color = {}, type = {})'.format(
            self.color, self.type_to_str(self.type))

    @classmethod
    def type_to_str(cls, type):
        for k, v in cls.type_names.items():
            if v == type:
                return k
        return "unknown"

    def is_alert(self):
        return self.type == self.TYPE_ALERT


class StatusGroup:
    def __init__(self, group_dict):
        if 'group' not in group_dict:
            raise MalformedConfig("missing 'group' name")
        elif 'status' not in group_dict:
            raise MalformedConfig("missing 'status' for group {}".format(
                group_dict['group']))
        elif not isinstance(group_dict['status'], dict):
            raise MalformedConfig("'status' must be a list an object")

        log.debug("status: {}".format(group_dict['status']))

        self.name = group_dict['group']
        self.status = {}

        for k, v in group_dict['status'].items():
            if 'color' not in v or 'type' not in v:
                log.debug("malformed entry for {}: {}".format(k, v))
                raise MalformedConfig(
                    "'status' entry for '{}' requires "
                    "a 'color' and a 'type'".format(k))
            try:
                sc = StatusColor(v['color'], v['type'])
                log.debug("status color: {}".format(sc))
            except MalformedConfig as e:
                print("error: malformed config for "
                      "status group '{}': {}".format(k, e))
                continue
            self.status[k] = sc

        self.hue_group = None

    def __repr__(self):
        return 'StatusGroup(name = "{}", {} status entries)'.format(
            self.name, len(self.status))

    def set_hue_group(self, grp):
        logging.debug("setting hue group {}".format(grp))
        self.hue_group = grp


def test_read_config(path):
    with open(path, 'r') as f:
        import json
        j = json.load(f)
        return j
    return None


def test_init_logging():
    logger.initDebugLogger()


def test_get_config(path):
    test_init_logging()
    j = test_read_config(path)
    c = Config()
    c.assimilate('test', j)
    return c


class Config:

    def __init__(self):
        self._name = None
        self._config = None
        self._user = None
        self._address = None
        self._groups = {}

    @classmethod
    def create(cls, name, cfg):
        c = cls()
        c.assimilate(name, cfg)
        return c

    def assimilate(self, name, cfg):
        log.debug('config assimilate: {} = {}'.format(name, cfg))

        if (not isinstance(name, str) and not isinstance(name, unicode)) or \
           len(name) == 0:
            raise MalformedConfig("expecting a non-zero string as name")

        if isinstance(cfg, str) or isinstance(cfg, unicode):
            try:
                cfg = json.loads(cfg)
            except SyntaxError as e:
                raise MalformedConfig("malformed json config")
        elif not isinstance(cfg, dict):
            raise MalformedConfig("expected a 'str' or a 'dict'")

        user = None
        addr = None
        groups = {}
        if 'user' in cfg:
            user = cfg['user']
            assert isinstance(user, str) or isinstance(user, unicode)
        if 'address' in cfg:
            addr = cfg['address']
            assert isinstance(addr, str) or isinstance(addr, unicode)

        if 'status_groups' not in cfg:
            raise MalformedConfig("no status groups defined in config")

        group_lst = cfg['status_groups']
        if not isinstance(group_lst, list):
            raise MalformedConfig("expected a list of status groups")

        for grp in group_lst:
            try:
                sg = StatusGroup(grp)
            except MalformedConfig as e:
                print('error: malformed group: {}'.format(e))
                raise MalformedConfig(e)
            groups[sg.name] = sg

        self._config = cfg
        self._user = user
        self._address = addr
        self._groups = groups

    def set_user(self, val):
        assert isinstance(val, str) or isinstance(val, unicode)
        self._user = val

    def set_address(self, val):
        assert isinstance(val, str) or isinstance(val, unicode)
        self._address = val

    def has_address(self):
        if self._address is not None:
            assert isinstance(self._address, str) or \
                isinstance(self._address, unicode)
            return len(self._address) > 0
        return False

    def has_user(self):
        if self._user is not None:
            assert isinstance(self._user, str) or \
                isinstance(self._user, unicode)
            return len(self._user) > 0
        return False

    def get_address(self):
        return self._address

    def get_user(self):
        return self._user

    def get_name(self):
        return self._name

    def get_raw_config(self):
        return self._config
