#!/usr/bin/python

import time
import random
import struct
import gevent
from gevent import socket

import sys, os
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
import ptt.rr_pb2 as rr_pb2
import frontend.etpacket as etpacket

def randomCtx():
    ctxs = [None,'rel','std','unexists']
    return ctxs[ random.randint(0,len(ctxs) - 1) ]

def randomAccount():
    accounts = [None,'1','18612345678','badaccount']
    return accounts[ random.randint(0,len(accounts) - 1) ]

def client():
    sock = socket.socket(socket.AF_INET,socket.SOCK_DGRAM)
    remote = ("127.0.0.1",5002)
    for i in range(1,5000):
        req = rr_pb2.QueryServer()
        req.version = '1'

        a = randomAccount()
        if a is not None:
            req.account = a

        c = randomCtx()
        if c is not None:
            req.system.context = c
            req.system.os = 'Linux'
        
        sock.sendto( etpacket.pack(req),remote)
        packet,address = sock.recvfrom(1024)
        packetlen,rep = etpacket.unpack(packet)
        
        assert rep is not None
        assert isinstance(rep,rr_pb2.QueryServerAck)
        assert rep.result == 0
        assert len(rep.servers) > 0
        #server = rep.servers[0]
        #assert socket.inet_ntoa(struct.pack('!I',server.ip)) == '219.148.21.125'
        #assert socket.ntohs(server.port) == 10003
    sock.close()


start = time.time()
threads = []
for i in range(0,10):
    threads.append(gevent.spawn(client))

gevent.joinall(threads)

elapsed = time.time() - start

print('Elapsed %f second,averge tps = %f' % (elapsed,5000 * 10 / elapsed))

