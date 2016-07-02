# -*- coding: UTF-8 -*-

import logging
import signal
import zmq

from dbtypes import *
from dispatchdb import DispatchDB
from generator import GenerateDispatchDB
from zmsg import pb_recv,pb_router_recv
import disp_pb2

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
            platform = self.db.ctx_map.get(self.defaultCtx(),None)
            if platforma is None:
                logging.error('default ctx invalid')
                return None
    
        user = platform.getUser(account)
        if user is None:
            return self.db.ctx_default.get(ctx,None)

        server = platform.dispatch(user.company)
        if server is None:
            server = self.db.ctx_default.get(ctx,None)
        return server


    def onPipe(self,watcher,revents):
        data = watcher.fd.read(32)
        # TODO get new DispatchDB
        pass

    def handleRequest(self,sock):
        while True:
            envelope,request= pb_router_recv(sock)
            if envelope is None or request is None:
                return

            if request.DESCRIPTOR.full_name != 'dispatch.Request':
                logging.warning('unsupport request %s from %s' % (request.DESCRIPTOR.full_name,str(address)))
                return
            ctx = None
            account = None

            if request.HasField('ctx'):
                ctx = request.ctx
            if request.HasField('account'):
                account = request.account

            server = self.dispatch(ctx,account)
            logging.debug('request ctx=%s,account=%s, return %s' % (ctx,account,str(server)))

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
        zctx = zmq.Context()
        try:
            svcsock = zctx.socket(zmq.ROUTER)
            svcsock.bind(self.options.get('address'))

            poller = zmq.Poller()
            poller.register(svcsock,zmq.POLLIN)

            while True:
                events = dict(poller.poll())
                if len(events) == 0:
                    logging.debug('poller 0')
                    continue
                if svcsock in events:
                    self.handleRequest(svcsock)
        except zmq.ContextTerminated as e:
            logging.info('ZMQ context terminated')
        finally:
            if not zctx.closed:
                if svcsock is not None:
                    svcsock.close()


