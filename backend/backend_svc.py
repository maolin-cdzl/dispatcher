# -*- coding: UTF-8 -*-

import logging
import signal
import string
import random
import zmq

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

_exit_address = 'inproc://%s' % (''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(6)))
_exit_sock = _zctx.socket(zmq.PUB)
_exit_sock.bind(_exit_address)

def backend_exit(signum,frame):
    logging.debug('backend_exit,signal:%d' % signum)
    _exit_sock.send('exit')

signal.signal(signal.SIGINT,backend_exit)
signal.signal(signal.SIGTERM,backend_exit)

def defaultCtx():
    return _options.get('default_ctx',None)

def dispatch(ctx,account):
    if ctx is None:
        ctx = defaultCtx()

    platform = _ddb.ctx_map.get(ctx,None)
    if platform is None:
        ctx = defaultCtx()
        platform = _ddb.ctx_map.get(ctx,None)
        if platform is None:
            logging.error('default ctx invalid')
            return None

    user = platform.getUser(account)
    if user is None:
        return _ddb.ctx_default.get(ctx,None)

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
    except Exception as e:
        logging.error('Exception: {0}'.format(e))
    finally:
        sock_exit_watcher.close()
        if _generator is not None:
            _generator.stop()
            _generator = None
        _ddb = None


