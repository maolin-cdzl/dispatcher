# -*- coding: UTF-8 -*-

import logging
import signal
import gevent
from gevent import socket
import zmq.green as zmq

from ptt import ptt_pb2,rr_pb2
from disp import disp_pb2
from disp import zprotobuf 
from etpacket import *

zctx = zmq.Context()
sock_be = zctx.socket(zmq.DEALER)
sock_svc = socket.socket(socket.AF_INET,socket.SOCK_DGRAM)

def pollReply(options):
    backends = options.get('backend-address')
    if isinstance(backends,str):
        sock_be.connect(backends)
        logging.info('connect backend: %s' % backends)
    elif isinstance(backends,list):
        for addr in backends:
            sock_be.connect(addr)
            logging.info('connect backend: %s' % addr)
    else:
        raise RuntimeError('Unknown backend-address type: %s' % type(backends).__name__)
    while True:
        envelope,msg = zprotobuf.pb_recv(sock_be)
        logging.debug('pollReply recv return')
        if msg is not None:
            if isinstance(msg,disp_pb2.Reply):
                if msg.HasField('server_ip') and msg.HasField('server_port') and msg.HasField('client_ip') and msg.HasField('client_port'):
                    smsg = ptt.ptt_pb2.Server()
                    smsg.ip = socket.inet_aton(msg.server_ip)
                    smsg.port = socket.htons(msg.server_port)
                    reply = ptt.rr_pb2.QueryServerAck()
                    reply.result = 0
                    reply.servers = [ smsg ]

                    packet = etpacket.pack(reply)
                    sock.sendto(packet,(msg.client_ip,msg.client_port))
                    logging.debug('Return service %s:%d to client %s:%d' % (msg.server_ip,msg.server_port,msg.client_ip,msg.client_port))
                else:
                    logging.warn('Backend reply uncompleted')


def pollRequest(options):
    sock_svc.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1)
    sock_svc.bind(options.get('frontend-address'))
    logging.info('Bind frontend on: %s' % str(options.get('frontend-address')))

    while True:
        packet,address = sock_svc.recvfrom(1024)
        logging.debug('pollRequest recv return')
        if packet is None or address is None:
            continue 
        packetlen,msg = etpacket.unpack(packet)
        if msg is None:
            continue 

        if isinstance(msg,ptt_pb2.QueryServer):
            req = disp_pb2.Request()
            req.client_ip = address[0]
            req.client_port = address[1]
            if msg.HasField('account'):
                req.account = msg.account
            if msg.HasField('system'):
                if msg.system.HasField('context'):
                    req.ctx = msg.system.context
            sock_be.send('',zmq.SNDMORE)
            sock_be.send_string(req.DESCRIPTOR.full_name,zmq.SNDMORE)
            sock_be.send(req.SerializeToString())
        else:
            logging.warning('unsupport request %s from %s' % (msg.DESCRIPTOR.full_name,str(address)))


def run(options):
    #gevent.signal(signal.SIGINT,gevent.shutdown)
    #gevent.signal(signal.SIGTERM,gevent.shutdown)
    backend_svc = gevent.spawn(pollReply,options)
    frontend_svc = gevent.spawn(pollRequest,options)
    gevent.joinall([backend_svc,frontend_svc])

