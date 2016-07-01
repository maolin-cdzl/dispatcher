#!/usr/bin/python

import os
import logging
from logging import Formatter
from logging.handlers import TimedRotatingFileHandler
import daemon
from daemon import runner

from generator import *

def SetupLogger(options):
    FORMAT = "%(asctime)-15s %(levelname)-8s %(filename)-16s %(message)s"
    formatter = Formatter(fmt=FORMAT)
    logger = logging.getLogger()

    if options.get('debug',False):
        handler = logging.StreamHandler()
        logger.setLevel(logging.DEBUG)
    else:
        handler = TimedRotatingFileHandler('%s/dispatcher.log' % options.get('root_path'),when="d",interval=1,backupCount=7)
        logger.setLevel(logging.INFO)

    handler.setFormatter(formatter)
    logger.addHandler(handler)


test_db = {
    'server': '222.222.46.204:9033',
    'user': 'test',
    'password': 'echat_test',
    'database': 'test'
}

std_db = {
    'server': '119.254.209.41:9433',
    'user': 'echat_log',
    'password': 'echat_log',
    'database': 'echat_r1'
}

class App:
    def __init__(self,options):
        self.options = options
        self.stdin_path = '/dev/null'
        self.stdout_path = '/dev/null'
        self.stderr_path = '/dev/null'
        self.pidfile_path = '%s/dispatcher.pid' % options.get('root_path')
        self.pidfile_timeout = 5
        self.svcs = []

    def run(self):
        udb = GenerateUDB(test_db)

options = {
    'root_path': os.path.dirname(os.path.abspath(__file__)),
    'debug': True,
    'default_ctx': 'rel',
}

if __name__ == '__main__':
    SetupLogger(options)
    app = App(options)

    if options.get('debug',False):
        app.run()
    else:
        daemon_runner = runner.DaemonRunner(app)
        daemon_runner.do_action()

