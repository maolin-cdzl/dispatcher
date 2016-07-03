#!/usr/bin/python

import os
import logging
from logging import Formatter
from logging.handlers import TimedRotatingFileHandler
import daemon
from daemon import runner

from backend.backend_svc import BackendSvc

def SetupLogger(options):
    FORMAT = "%(asctime)-15s %(levelname)-8s %(filename)-16s %(message)s"
    formatter = Formatter(fmt=FORMAT)
    logger = logging.getLogger()

    if options.get('debug',False):
        handler = logging.StreamHandler()
        logger.setLevel(logging.DEBUG)
    else:
        handler = TimedRotatingFileHandler('%s/dispatch-backend.log' % options.get('root_path'),when="d",interval=1,backupCount=7)
        logger.setLevel(logging.INFO)

    handler.setFormatter(formatter)
    logger.addHandler(handler)


class App:
    def __init__(self,options):
        self.options = options
        self.stdin_path = '/dev/null'
        self.stdout_path = '/dev/null'
        self.stderr_path = '/dev/null'
        self.pidfile_path = '%s/dispatch-backend.pid' % options.get('root_path')
        self.pidfile_timeout = 5
        self.svcs = []

    def run(self):
        svc = BackendSvc(self.options)
        svc.start()

options = {
    'root_path': os.path.dirname(os.path.abspath(__file__)),
    'debug': True,
    'default_ctx': 'rel',
    'backend-address': 'tcp://127.0.0.1:5000',
    'ruledb': {
        'type': 'mysql',
        'server': 'localhost',
        'user': 'dispatcher',
        'password': 'shanlitech@231207',
        'database': 'dispatch'
    },
    'sync_period' : 60,
}

if __name__ == '__main__':
    SetupLogger(options)
    app = App(options)

    if options.get('debug',False):
        app.run()
    else:
        daemon_runner = runner.DaemonRunner(app)
        daemon_runner.do_action()

