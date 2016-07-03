# -*- coding: UTF-8 -*-

import logging
import signal
import zmq

from dbtypes import *
from dispatchdb import DispatchDB
from generator import GenerateDispatchDB,DDBGenerator
from disp import disp_pb2
from disp import zprotobuf 

zctx = zmq.Context()

def backend_exit(signum,frame):
    print('backend_exit')
    if not zctx.closed:
        print('try terminate zctx')
        zctx.term()

signal.signal(signal.SIGINT,backend_exit)
signal.signal(signal.SIGTERM,backend_exit)

class BackendSvc:
    def __init__(self,options):
        self.options = options
        self.db = None

    def setDB(self,db):
        self.db = db

    def defaultCtx(self):
        return self.options.get('default_ctx',None)

    def dispatch(self,ctx,account):
        if ctx is None:
            ctx = self.defaultCtx()

        platform = self.db.ctx_map.get(ctx,None)
        if platform is None:
            ctx = self.defaultCtx()
            platform = self.db.ctx_map.get(ctx,None)
            if platform is None:
                logging.error('default ctx invalid')
                return None
    
        user = platform.getUser(account)
        if user is None:
            return self.db.ctx_default.get(ctx,None)

        server = platform.dispatch(user.company)
        if server is None:
            server = self.db.ctx_default.get(ctx,None)
        return server

    def handleRequest(self,sock):
        #while True:
        # poller act like 'level trigger' ?
        envelope,request= zprotobuf.pb_router_recv(sock,zmq.NOBLOCK)
        if envelope is None or request is None:
            logging.warning('Request recv error')
            return

        if request.DESCRIPTOR.full_name != 'dispatch.Request':
            logging.warning('unsupport request %s' % (request.DESCRIPTOR.full_name))
            return
        ctx = None
        account = None

        if request.HasField('ctx'):
            ctx = request.ctx
        if request.HasField('account'):
            account = request.account

        server = self.dispatch(ctx,account)
        #logging.debug('request ctx=%s,account=%s, return %s' % (ctx,account,str(server)))

        reply = disp_pb2.Reply()
        reply.server_ip = server.ip
        reply.server_port = server.port
        if request.HasField('client_ip'):
            reply.client_ip = request.client_ip
        if request.HasField('client_port'):
            reply.client_port = request.client_port
        
        sock.send_multipart(envelope,zmq.SNDMORE)
        sock.send_string(reply.DESCRIPTOR.full_name,zmq.SNDMORE)
        sock.send( reply.SerializeToString() )
        

    def start(self):
        self.db = GenerateDispatchDB(self.options.get('ruledb'))
        if self.db is None:
            logging.fatal('Can not read database')
            return
        gt = DDBGenerator(zctx,ruledb=self.options.get('ruledb'),period=self.options.get('sync_period'))
        svcsock = zctx.socket(zmq.ROUTER)
        try:
            gt.start()
            svcsock.bind(self.options.get('backend-address'))

            poller = zmq.Poller()
            poller.register(svcsock,zmq.POLLIN)
            poller.register(gt.socket(),zmq.POLLIN)

            while True:
                events = dict(poller.poll())
                logging.debug('poll with events: %d' % len(events))
                if svcsock in events:
                    self.handleRequest(svcsock)
                if gt.socket() in events:
                    gt.socket().recv()
                    ddb = gt.pop()
                    if ddb is not None:
                        self.db = ddb
                        logging.info('Update database')
        except zmq.ContextTerminated as e:
            logging.info('ZMQ context terminated')
        except RuntimeError as e:
            logging.error('RuntimeError: {0}'.format(e))
        except Exception as e:
            logging.error('Exception: {0}'.format(e))
        finally:
            if gt is not None:
                gt.stop()
            if not zctx.closed:
                if svcsock is not None:
                    svcsock.close()


