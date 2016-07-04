#!/usr/bin/python

import os
import logging
from logging import Formatter
from logging.handlers import TimedRotatingFileHandler
import daemon
from daemon import runner

def SetupLogger(options):
    FORMAT = "%(asctime)-15s %(levelname)-8s %(filename)-16s %(message)s"
    formatter = Formatter(fmt=FORMAT)
    logger = logging.getLogger()

    if options.get('debug',False):
        handler = logging.StreamHandler()
        logger.setLevel(logging.DEBUG)
    else:
        handler = TimedRotatingFileHandler('%s/dispatch-frontend.log' % options.get('root_path'),when="d",interval=1,backupCount=7)
        logger.setLevel(logging.INFO)

    handler.setFormatter(formatter)
    logger.addHandler(handler)


class App:
    def __init__(self,options):
        self.options = options
        self.stdin_path = '/dev/null'
        self.stdout_path = '/dev/null'
        self.stderr_path = '/dev/null'
        self.pidfile_path = '%s/dispatch-frontend.pid' % options.get('root_path')
        self.pidfile_timeout = 5
        self.svcs = []

    def run(self):
        from frontend import frontend_svc
        SetupLogger(self.options)
        try:
            logging.info('dispatcher frontend start')
            frontend_svc.run(self.options)
            logging.info('dispatcher frontend exit')
        except Exception as e:
            logging.error('Exception: {0}'.format(e))

options = {
    'root_path': os.path.dirname(os.path.abspath(__file__)),
    'debug': False,
    'backend-address': 'tcp://127.0.0.1:5000',
    'frontend-address': ('127.0.0.1',5002)
}

if __name__ == '__main__':
    app = App(options)

    if options.get('debug',False):
        app.run()
    else:
        daemon_runner = runner.DaemonRunner(app)
        daemon_runner.do_action()

