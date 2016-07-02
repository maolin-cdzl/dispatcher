# -*- coding: UTF-8 -*-

import logging
import signal
import socket
import pyev

from dbtypes import *
from dispatchdb import DispatchDB
from etpacket import *

class Dispatcher:
    def __init__(self,options):
        self.options
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

    def listen(self):
        s = socket.socket(socket.AF_INET,socket.SOCK_DGRAM)
        s.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1)
        s.bind(self.options.get('address'))
        s.setblocking(0)
        return s

    def sig_cb(self,watcher,revents):
        logging.info('signal: %d' % revents)
        watcher.loop.stop(pyev.EVBREAK_ALL)

    def onPipe(self,watcher,revents):
        data = watcher.fd.read(32)
        # TODO get new DispatchDB
        pass

    def onRequest(self,watcher,revents):
        if 0 == (pyev.EV_READ & revents):
            return
        packet,address = watcher.fd.recvfrom(1024)
        if packet is None or address is None:
            return
        packetlen,msg = etpacket.unpack(packet)
        if msg is None:
            return

        if msg.DESCRIPTOR.full_name != 'ptt.rr.QueryServer':
            logging.warning('unsupport request %s from %s' % (msg.DESCRIPTOR.full_name,str(address)))
            return
        ctx = None
        account = None

        if msg.HasField('account'):
            account = msg.account
        if msg.HasField('system'):
            if msg.system.HasField('context'):
                ctx = msg.system.context

        server = self.dispatch(ctx,account)
        logging.info('request from %s,ctx=%s,account=%s, return %s' % (str(address),ctx,account,str(server)))

        smsg = ptt.ptt_pb2.Server()
        smsg.ip = server.net_ip
        smsg.port = server.net_port
        msg = ptt.rr_pb2.QueryServerAck()
        msg.result = 0
        msg.servers = [ smsg ]

        packet = etpacket.pack(msg)
        watcher.fd.sendto(packet,address)
        

    def start(self):
        self.db = GenerateDispatchDB(self.options.get('ruledb'))
        if self.db is None:
            logging.fatal('Can not read database')
            return
        loop = pyev.default_loop()

        try:
            s = listen()
            s0,s1= socket.socketpair()

            io_s = loop.io(s,pyev.EV_READ,self.onRequest)
            io_s.start()
            io_s0 = loop.io(s0,pyev.EV_READ,self.onPipe)
            io_s0.start()

            sig_int = loop.signal(signal.SIGINT,self.sig_cb)
            sig_int.start()
            sig_term = loop.signal(signal.SIGTERM,self.sig_cb)
            sig_term.start()

            loop.start()
        finally:
            if s is not None:
                s.close()
            if s0 is not None:
                s0.close()
            if s1 is not None:
                s1.close()


