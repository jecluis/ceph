from enum import Enum
import requests
import json
from . import logger
from .logger import log

WITH_HTTP_DEBUG = False


def set_http_debug():
    global WITH_HTTP_DEBUG
    WITH_HTTP_DEBUG = True

    """
    import logging
    if log.logger is None:
        logging.basicConfig(level=logging.DEBUG)
        _log = logging.getLogger()
        setLogger(_log)
    """
    logger.initDebugLogger()

    import logging
    import httplib as http_client
    http_client.HTTPConnection.debuglevel = 1
    requests_log = logging.getLogger("requests.packages.urllib3")
    requests_log.setLevel(logging.DEBUG)
    requests_log.propagate = True


class HueErrors(Enum):
    UNAUTHORIZED_USER = 1
    INVALID_BODY = 2
    UNAVAILABLE_RESOURCE = 3
    UNAVAILABLE_METHOD = 4
    MISSING_PARAMETER = 5
    UNAVAILABLE_PARAMETER = 6
    INVALID_PARAMETER = 7
    NOT_MODIFIABLE_PARAMTER = 8
    TOO_MANY_ITEMS = 11
    REQUIRES_PORTAL_CONNECTION = 12
    INTERNAL_ERROR = 901
    # command-specific errors
    LINK_BUTTON_NOT_PRESSED = 101


class HueColors:
    # source: https://www.developers.meethue.com/documentation/core-concepts
    RED = {"hue": 0, "sat": 254, "bri": 254}
    YELLOW = {"hue": 12750, "sat": 254, "bri": 254}
    GREEN = {"hue": 25500, "sat": 254, "bri": 254}

    names = {"red": RED, "yellow": YELLOW, "green": GREEN}

    @classmethod
    def by_name(cls, name):
        n = name.lower()
        if n not in cls.names:
            raise KeyError("'{}' it not a known color".format(n))
        return cls.names[n]


class HueGroup:
    def __init__(self, gid, ginfo):
        assert gid
        assert ginfo
        assert len(ginfo) > 0
        assert 'name' in ginfo
        assert 'lights' in ginfo

        self.name = ginfo['name']
        self.lights = ginfo['lights']
        self.gid = gid
        self.raw_data = ginfo

    def __repr__(self):
        return 'HueGroup( id = {}, name = {}, lights = {}, raw = {}'.format(
            self.gid, self.name, self.lights, self.raw_data)


class HueError(Exception):
    pass


class HueBridge:

    URL = 'http://{addr}/api/{endpoint}'
    HEADERS = {"Content-Type": "application/json"}
    DEVICE_TYPE = "ceph-mgr#status-to-hue"

    # endpoint-name: (endpoint, method, authenticated?)
    # if authenticated, requires a 'user' before the endpoint; e.g.,
    #   http://{addr}/api/{user}/{endpoint} if authenticated
    ENDPOINTS = {
        "user-create": (None, "post", False),
        "user-check": (None, "get", True),
        "groups-get": ("groups/", "get", True),
        "group-set": ("groups/{gid}/action/", "put", True)
    }

    def __init__(self, address, user):
        self.address = address
        self.user = user

    @classmethod
    def get_endpoint(cls, action_name, **kwargs):

        if action_name not in cls.ENDPOINTS:
            raise HueError("invalid endpoint name '{}'".format(action_name))
        (raw_ep, method, auth) = cls.ENDPOINTS[action_name]
        log.debug("get_endpoint: ep = '{}', method = '{}', "
                  "auth = '{}'".format(raw_ep, method, auth))
        ep = ""
        if raw_ep is not None:
            ep = raw_ep.format(kwargs)

        if auth:
            if 'user' not in kwargs:
                raise KeyError("endpoint for '{}' requires a user, "
                               "but none specified".format(action_name))
            ep = '{user}/{ep}'.format(user=kwargs["user"], ep=ep)
        return (ep, method)

    @classmethod
    def get_url(cls, action_name, **kwargs):
        if 'addr' not in kwargs:
            raise KeyError("address has not been specified")
        (ep, method) = cls.get_endpoint(action_name, **kwargs)
        url = cls.URL.format(addr=kwargs["addr"],
                             endpoint=ep)
        return (url, method)

    def get_user(self):
        return self.user

    def get_address(self):
        return self.address

    @staticmethod
    def is_error_response(response):
        if response.status_code != 200:
#           logging.error("unexpected response: {}".format(response))
            return True
        return False

    @staticmethod
    def is_resource(val):
        return isinstance(val, dict)

    @staticmethod
    def is_error(val):
        if not isinstance(val, dict):
            raise TypeError("expecting a dictionary value")
        return "error" in val

    @staticmethod
    def is_success(val):
        if not isinstance(val, dict):
            raise TypeError("expecting a dictionary value")
        return "success" in val

    @staticmethod
    def check_has_errors(lst):
        if isinstance(lst, dict):
            return HueBridge.is_error(lst)
        for entry in lst:
            if HueBridge.is_error(entry):
                return True
        return False

    @staticmethod
    def get_errors(lst):
        if isinstance(lst, dict):
            return [] if not HueBridge.is_error(lst) else [lst]
        return [entry for entry in lst if 'error' in entry]

    @staticmethod
    def _get_type(d):
        return d['type'] if isinstance(d, dict) and 'type' in d else None

    @staticmethod
    def error_match(err, expected):
        if isinstance(err, int):
            val = err
        elif isinstance(err, list):
            val = HueBridge._get_type(lst[0]) if len(lst) > 0 else {}
        elif isinstance(err, dict):
            val = HueBridge._get_type(err)
        else:
            raise TypeError("'err' should be a dictionary")
        if not val:
            return False
        return HueErrors(val) == expected

    @staticmethod
    def is_error_response(response):
        return True if response.status_code != 200 else False

    @classmethod
    def do_request(cls, url, method, data=None):

        log.debug("url = '{}', method = '{}', data = '{}'".format(
            url, method, data))

        if data is not None:
            if isinstance(data, dict):
                data = json.dumps(data)
            elif not isinstance(data, str):
                raise ValueError("expected 'data' as a dict or str")

        response = None
        if method == "get":
            response = requests.get(url, headers=cls.HEADERS)
        elif method == "post":
            response = requests.post(url, headers=cls.HEADERS, data=data)
        elif method == "put":
            response = requests.put(url, headers=cls.HEADERS, data=data)
        elif method == "delete":
            reponse = requests.delete(url, headers=cls.HEADERS, data=data)

        if cls.is_error_response(response):
            raise BridgeError(
                "unable to perform request: {}".format(response))

        res_json = response.json()
        errors = None
        if cls.check_has_errors(res_json):
            errors = cls.get_errors(res_json)

        return (res_json, errors)

    @classmethod
    def user_create(cls, address):
        (url, method) = cls.get_url("user-create", addr=address)
        assert len(url) > 0
        assert len(method) > 0

        log.debug("user_create: url = '{}', addr = '{}'".format(
            url, address))

        data = {'devicetype': cls.DEVICE_TYPE}
        (res, errors) = cls.do_request(url, method, data=data)
        if errors:
            assert len(errors) == 1
            if cls.error_match(errors[0], HueErrors.LINK_BUTTON_NOT_PRESSED):
                error_msg = "Bridge's link button not pressed."
            else:
                error_msg = "unexpected error response: {}".format(errors[0])
            raise HueError(error_msg)

        assert errors is None
        assert len(res) == 1
        assert 'success' in res[0]
        res = res[0]['success']
        assert 'username' in res
        assert len(res['username']) > 0
        return res['username']

    def get_groups(self):
        log.debug('obtaining bridge groups from {} with user {}'.format(
            self.address, self.user))
        (url, method) = self.get_url('groups-get',
                                     user=self.user,
                                     addr=self.address)
        log.debug('using url {}, method {}'.format(url, method))
        (res, errors) = self.do_request(url, method)
        if errors:
            log.debug('found errors: {}'.format(errors))
            return {}

        return res

    def _get_group_id(self, group):
        groups = self.get_groups()
        for gid, info in groups.items():
            if info['name'] == group:
                return gid
        return None

    def set_group_state(self, group, color):
        log.debug('setting group {} color to {}'.format(group, color))
        group_id = self._get_group_id(group)
        if group_id is None:
            log.debug('group {} not found'.format(group))
            return False

        (url, method) = self.get_url('groups-set',
                                     user=self.user,
                                     addr=self.address,
                                     gid=group_id)
        log.debug('using url {}, method {}'.format(url, method))
        (res, errors) = self.do_request(url, method, data=color)
        if errors:
            log.debug('found errors: {}'.format(errors))
            return False
        return True

    def user_exists(self, config):
        log.debug("checking if user '{}' exists".format(self.user))
        endpoint = 'api/{}'.format(self.user)
        url = 'http://{addr}/{ep}'.format(
            addr=config['address'], ep=endpoint)
        log.debug("requesting GET to {}".format(url))
        response = requests.get(url)

    #    logging.debug("response: code {}, json: {}".format(
    #        response.status_code, response.json()))

        if is_error_response(response):
            return False

        json = response.json()

        if self.is_error(json):
            error = get_error(json)
            if HueErrors(error['type']) == HueErrors.UNAUTHORIZED_USER:
                return False
            log.error("unexpected error: {}".format(error))
            assert False
        return True

