# -*- coding: UTF-8 -*-

import os
import logging
import struct
import signal
import gevent
from gevent import socket
import zmq.green as zmq

from ptt import ptt_pb2,rr_pb2
from disp import disp_pb2
from disp import zprotobuf 
import etpacket

zctx = zmq.Context()
sock_be = zctx.socket(zmq.DEALER)
sock_svc = socket.socket(socket.AF_INET,socket.SOCK_DGRAM)
_options = None

# copy from zmq.auth
def load_certificate(filename):
    """Load public and secret key from a zmq certificate.
    
    Returns (public_key, secret_key)
    
    If the certificate file only contains the public key,
    secret_key will be None.
    
    If there is no public key found in the file, ValueError will be raised.
    """
    public_key = None
    secret_key = None
    if not os.path.exists(filename):
        raise IOError("Invalid certificate file: {0}".format(filename))

    with open(filename, 'rb') as f:
        for line in f:
            line = line.strip()
            if line.startswith(b'#'):
                continue
            if line.startswith(b'public-key'):
                public_key = line.split(b"=", 1)[1].strip(b' \t\'"')
            if line.startswith(b'secret-key'):
                secret_key = line.split(b"=", 1)[1].strip(b' \t\'"')
            if public_key and secret_key:
                break
    
    if public_key is None:
        raise ValueError("No public key found in %s" % filename)
    
    return public_key, secret_key

def getExistsPath(base_dir,path):
    p = os.path.join(base_dir,path)
    if os.path.exists(p):
        return p
    p = os.path.join(path)
    if os.path.exists(p):
        return p
    logging.critical('Can not found path: %s' % path)
    raise RuntimeError('Can not found path: %s' % path)

def setup_auth():
    auth = _options.get('auth',None)
    if auth is None:
        return
    base_dir = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)),'..'))
    cert_dir = getExistsPath(base_dir,auth.get('cert_dir','cert'))
    client_secret_file = os.path.join(cert_dir, auth.get('private_key','client.key_secret'))
    client_public, client_secret = load_certificate(client_secret_file)
    sock_be.curve_secretkey = client_secret
    sock_be.curve_publickey = client_public

    server_public_file = os.path.join(cert_dir, auth.get('server_public_key','server.key'))
    server_public, _ = load_certificate(server_public_file)
    # The client must know the server's public key to make a CURVE connection.
    sock_be.curve_serverkey = server_public

def pollReply():
    setup_auth()
    backends = _options.get('backend-address')
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
#        logging.debug('pollReply recv return')
        if msg is not None:
            if isinstance(msg,disp_pb2.Reply):
                if msg.HasField('client_ip') and msg.HasField('client_port'):
                    reply = rr_pb2.QueryServerAck()
                    reply.result = msg.result
                    if msg.HasField('server_ip') and msg.HasField('server_port'):
                        server = reply.servers.add()
                        server.ip = struct.unpack('!I',socket.inet_aton(msg.server_ip))[0]
                        server.port = socket.htons(msg.server_port)

                    packet = etpacket.pack(reply)
                    sock_svc.sendto(packet,(msg.client_ip,msg.client_port))
#                    logging.debug('Return service %s:%d to client %s:%d' % (msg.server_ip,msg.server_port,msg.client_ip,msg.client_port))
                else:
                    logging.warn('Backend reply uncompleted')


def pollRequest():
    sock_svc.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1)
    sock_svc.bind(_options.get('frontend-address'))
    logging.info('Bind frontend on: %s' % str(_options.get('frontend-address')))

    while True:
        packet,address = sock_svc.recvfrom(1024)
#        logging.debug('pollRequest recv return')
        if packet is None or address is None:
            continue 
        packetlen,msg = etpacket.unpack(packet)
        if msg is None:
            continue 

        if isinstance(msg,rr_pb2.QueryServer):
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
    global _options
    _options = options
    tasks = []
    tasks.append( gevent.spawn(pollReply) )
    tasks.append( gevent.spawn(pollRequest) )
    gevent.signal(signal.SIGINT,gevent.killall,tasks)
    gevent.signal(signal.SIGTERM,gevent.killall,tasks)
    gevent.joinall(tasks)

