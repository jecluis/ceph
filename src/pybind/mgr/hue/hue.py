from enum import Enum


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
        ep = None
        if raw_ep is not None:
            ep = raw_ep.format(kwargs)

        if auth:
            if 'user' not in kwargs:
                raise KeyError("endpoint for '{}' requires a user, "
                               "but none specified".format(action_name))
            ep = '{user}/{ep}'.format(user=kwargs["user"], ep)
        return (ep, method, auth)

    @classmethod
    def get_url(cls, action_name, **kwargs):
        if 'addr' not in kwargs:
            raise KeyError("address has not been specified")
        (ep, method) = cls.get_endpoint(action_name, kwargs)
        url = cls.URL.format(addr=kwargs["addr"],
                             endpoint=ep)
        return (url, method)

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
            return is_error(lst)
        for entry in lst:
            if is_error(entry):
                return True
        return False

    @staticmethod
    def get_errors(lst):
        if isinstance(lst, dict):
            return [] if not is_error(lst) else [lst]
        return [entry for entry in lst if 'error' in entry]

    @staticmethod
    def error_match(err, expected):
        err_type = lambda d: d['type'] \
                (if isinstance(d, dict) and 'type' in d) else None

        if isinstance(err, int):
            val = err
        elif isinstance(err, lst):
            val = err_type(lst[0]) if len(lst) > 0 else {}
        elif isinstance(err, dict):
            val = err_type(err)
        else:
            raise TypeError("'err' should be a dictionary")
        if not val:
            return False
        return HueErrors(val) == expected

    @staticmethod
    def do_request(url, method, **kwargs):
        args = {}
        if 'data' in kwargs:
            args = {"data": json.dumps(kwargs["data"])}

        response = None
        if method == "get":
            response = requests.get(url, headers=HEADERS)
        elif method == "post":
            response = requests.post(url, headers=HEADERS, args)
        elif method == "put":
            response = requests.put(url, headers=HEADERS, args)
        elif method == "delete":
            reponse = requests.delete(url, headers=HEADERS, args)

        if is_error_response(response):
            raise BridgeError(f"unable to perform request: {response!r}")

        json = response.json()
        errors = None
        if check_has_errors(json):
            errors = get_errors(json)

        return (json, errors)

    @classmethod
    def user_create(cls, address):
        (url, method) = cls.get_url("user-create", addr=address)
        assert len(url) > 0
        assert len(method) > 0

        (res, errors) = cls.do_request(url, method, data=DEVICE_TYPE)
        if errors:
            assert len(errors) == 1
            if error_match(errors[0], HueErrors.LINK_BUTTON_NOT_PRESSED):
                error_msg = "Bridge's link button not pressed."
            else:
                error_msg = "unexpected error response: {errors[0]!r}"


    def user_exists(config):
        logging.debug("checking if user '{}' exists".format(self.user))
        endpoint = 'api/{}'.format(self.user)
        url = 'http://{addr}/{ep}'.format(
                addr=config['address'], ep=endpoint)
        logging.debug("requesting GET to {}".format(url))
        response = requests.get(url)

    #    logging.debug("response: code {}, json: {}".format(
    #        response.status_code, response.json()))

        if is_error_response(response):
            return False

        json = response.json()

        if is_error(json):
            error = get_error(json)
            if HueErrors(error['type']) == HueErrors.UNAUTHORIZED_USER:
                return False
            logging.error("unexpected error: {}".format(error))
            assert False
        return True

