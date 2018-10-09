class Logger:

    def __init__(self):
        self.logger = None

    def __getattr__(self, name):
        if self.logger is None:
            return self.stub

        try:
            return self.logger.__getattribute__(name)
        except AttributeError:
            return self.stub

    def stub(self, *args):
        # print("called stub() with '{}'".format(args))
        pass

    def setLogger(self, l):
        self.logger = l


def setLogger(l):
    global log
    log.setLogger(l)


def initDebugLogger():
    import logging
    logging.basicConfig(level=logging.DEBUG)
    _log = logging.getLogger()
    setLogger(_log)


log = Logger()
