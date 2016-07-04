# -*- coding: UTF-8 -*-

import os
import logging
import signal
import string
import random
import zmq
import zmq.auth
from zmq.auth.thread import ThreadAuthenticator

from dbtypes import *
from dispatchdb import DispatchDB
from generator import GenerateDispatchDB,DDBGenerator
from disp import disp_pb2
from disp import zprotobuf 

_zctx = zmq.Context()
_sock = None
_ddb = None
_options = None
_generator = None
_auth = None

_exit_address = 'inproc://%s' % (''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(6)))
_exit_sock = _zctx.socket(zmq.PUB)
_exit_sock.bind(_exit_address)

def backend_exit(signum,frame):
    logging.debug('backend_exit,signal:%d' % signum)
    _exit_sock.send('exit')

signal.signal(signal.SIGINT,backend_exit)
signal.signal(signal.SIGTERM,backend_exit)

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
    global _auth
    assert _options is not None
    auth = _options.get('auth',None)
    if auth is None:
        return
    base_dir = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)),'..'))
    try:
        _auth = ThreadAuthenticator(_zctx)
        _auth.start()
        whitelist = auth.get('whitelist',None)
        if whitelist is not None:
            _auth.allow(whitelist)
        public_path = auth.get('public_key_dir','public_keys')
        _auth.configure_curve(domain='*',location=getExistsPath(base_dir,public_path))
        private_dir = getExistsPath(base_dir,auth.get('private_key_dir','private_keys'))
        private_key = os.path.join(private_dir,auth.get('private_key_file','server.key_secret'))
        server_public,server_private = zmq.auth.load_certificate(private_key)
        _sock.curve_secretkey = server_private
        _sock.curve_publickey = server_public
        _sock.curve_server = True
    except:
        _auth.stop()
        _auth = None

def defaultCtx():
    return _options.get('default_ctx',None)

def dispatch(ctx,account):
    if ctx is None:
        ctx = defaultCtx()

    platform = _ddb.ctx_map.get(ctx,None)
    if platform is None:
        return -1

    if account == ctx:
        return _ddb.ctx_default.get(ctx,None)

    user = platform.getUser(account)
    if user is None:
        return -2

    server = platform.dispatch(user.company)
    if server is None:
        server = _ddb.ctx_default.get(ctx,None)
    return server

def handleRequest():
    assert _sock is not None
    #while True:
    # poller act like 'level trigger' ?
    envelope,request= zprotobuf.pb_router_recv(_sock,zmq.NOBLOCK)
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

    server = dispatch(ctx,account)
    #logging.debug('request ctx=%s,account=%s, return %s' % (ctx,account,str(server)))

    reply = disp_pb2.Reply()
    if isinstance(server,int):
        reply.result = server
    else:
        reply.result = 0
        reply.server_ip = server.ip
        reply.server_port = server.port
    if request.HasField('client_ip'):
        reply.client_ip = request.client_ip
    if request.HasField('client_port'):
        reply.client_port = request.client_port
    
    _sock.send_multipart(envelope,zmq.SNDMORE)
    _sock.send_string(reply.DESCRIPTOR.full_name,zmq.SNDMORE)
    _sock.send( reply.SerializeToString() )

def run(options):
    global _options,_ddb,_generator,_sock
    _options = options
    _ddb = GenerateDispatchDB(_options.get('ruledb'))
    if _ddb is None:
        logging.fatal('Can not initialize dispatch database')
        raise RuntimeError('Can not initialize dispatch database')

    _generator = DDBGenerator(_zctx,ruledb=_options.get('ruledb'),period=_options.get('sync_period'))
    _sock = _zctx.socket(zmq.ROUTER)
    sock_exit_watcher = _zctx.socket(zmq.SUB)
    try:
        _generator.start()
        setup_auth()
        _sock.bind(_options.get('backend-address'))
        sock_exit_watcher.connect(_exit_address)
        sock_exit_watcher.setsockopt(zmq.SUBSCRIBE,'')

        poller = zmq.Poller()
        poller.register(_sock,zmq.POLLIN)
        poller.register(_generator.socket(),zmq.POLLIN)
        poller.register(sock_exit_watcher,zmq.POLLIN)

        while True:
            events = dict(poller.poll())
            if _sock in events:
                handleRequest()
            if _generator.socket() in events:
                _generator.socket().recv()
                ddb = _generator.pop()
                if ddb is not None:
                    _ddb = ddb
                    logging.info('Update database')
            if sock_exit_watcher in events:
                sock_exit_watcher.recv()
                break
    except zmq.ContextTerminated as e:
        logging.info('ZMQ context terminated')
    except RuntimeError as e:
        logging.error('RuntimeError: {0}'.format(e))
#    except Exception as e:
#        logging.error('Exception: {0}'.format(e))
    finally:
        sock_exit_watcher.close()
        if _generator is not None:
            _generator.stop()
            _generator = None
        _ddb = None
        if _auth is not None:
            _auth.stop()


