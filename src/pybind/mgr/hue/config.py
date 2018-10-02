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
            logging.error("unknown color '{}'".format(name))
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

        logging.debug("status: {}".format(group_dict['status']))

        self.name = group_dict['group']
        self.status = {}

        for k, v in group_dict['status'].items():
            if 'color' not in v or 'type' not in v:
                logging.debug("malformed entry for {}: {}".format(k, v))
                raise MalformedConfig(
                    "'status' entry for '{}' requires "
                    "a 'color' and a 'type'".format(k))
            try:
                sc = StatusColor(v['color'], v['type'])
                logging.debug("status color: {}".format(sc))
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
