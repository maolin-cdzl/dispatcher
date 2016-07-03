#!/usr/bin/python

import time
import random
import gevent
import zmq.green as zmq

import sys, os
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'disp'))
import disp_pb2
import zprotobuf

context = zmq.Context()

def randomCtx():
    ctxs = [None,'rel','std','unexists']
    return ctxs[ random.randint(0,len(ctxs) - 1) ]

def randomAccount():
    accounts = [None,'1','18612345678','badaccount']
    return accounts[ random.randint(0,len(accounts) - 1) ]

def client():
    sock = context.socket(zmq.DEALER)
    sock.connect("tcp://127.0.0.1:5000")
    for i in range(1,5000):
        req = disp_pb2.Request()
        req.client_ip = '127.0.0.1'
        req.client_port = 12345

        c = randomCtx()
        if c is not None:
            req.ctx = c

        a = randomAccount()
        if a is not None:
            req.account = a

        sock.send('',zmq.SNDMORE)
        sock.send_string(req.DESCRIPTOR.full_name,zmq.SNDMORE)
        sock.send(req.SerializeToString())

        envelop,rep = zprotobuf.pb_recv(sock)
        assert rep is not None
        assert isinstance(rep,disp_pb2.Reply)
        assert rep.HasField('server_ip')
        assert rep.HasField('server_port')
        assert rep.client_ip == req.client_ip
        assert rep.client_port == req.client_port
    sock.close()


start = time.time()
threads = []
for i in range(0,10):
    threads.append(gevent.spawn(client))

gevent.joinall(threads)

elapsed = time.time() - start

print('Elapsed %f second,averge tps = %f' % (elapsed,5000 * 10 / elapsed))
