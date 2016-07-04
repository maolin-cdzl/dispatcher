#!/usr/bin/python

import os
import logging
from logging import Formatter
from logging.handlers import TimedRotatingFileHandler
import daemon
from daemon import runner
from backend_conf import options

root_path = os.path.dirname(os.path.abspath(__file__))

def SetupLogger():
    FORMAT = "%(asctime)-15s %(levelname)-8s %(filename)-16s %(message)s"
    formatter = Formatter(fmt=FORMAT)
    logger = logging.getLogger()

    if options.get('debug',False):
        handler = logging.StreamHandler()
        logger.setLevel(logging.DEBUG)
    else:
        log_dir = os.path.join(root_path,'log')
        if not os.path.exists(log_dir):
            os.mkdir(log_dir)
        handler = TimedRotatingFileHandler('%s/dispatch-backend.log' % log_dir,when="d",interval=1,backupCount=7)
        logger.setLevel(logging.INFO)

    handler.setFormatter(formatter)
    logger.addHandler(handler)


class App:
    def __init__(self):
        self.stdin_path = '/dev/null'
        self.stdout_path = '/dev/null'
        self.stderr_path = '/dev/null'
        self.pidfile_path = '%s/dispatch-backend.pid' % root_path
        self.pidfile_timeout = 5
        self.svcs = []

    def run(self):
        import backend.backend_svc as backend_svc
        backend_svc.run(options)

if __name__ == '__main__':
    SetupLogger()
    app = App()

    if options.get('debug',False):
        app.run()
    else:
        daemon_runner = runner.DaemonRunner(app)
        daemon_runner.do_action()

