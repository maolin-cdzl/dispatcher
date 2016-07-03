# -*- coding: UTF-8 -*-

import logging
from google.protobuf import symbol_database as _symbol_database
import disp_pb2

def zmsg_recv(sock,flags=0):
    try:
        frames = sock.recv_multipart(flags)
        if frames is None or len(frames) == 0:
            return (None,None)

        envelop_pos = 0
        while envelop_pos < len(frames):
            if len(frames[envelop_pos]) == 0:
                break
            envelop_pos += 1

        if envelop_pos == len(frames):
            return (None,frames)
        elif envelop_pos + 1 < len(frames):
            return (frames[:envelop_pos + 1],frames[envelop_pos + 1:])
        else:
            return (frames,None)
    except Exception as e:
        logging.error('recv exception: {0}'.format(e))
        return (None,None)

def zmsg_router_recv(sock,flags=0):
    try:
        frames = sock.recv_multipart(flags)
        if frames is None or len(frames) < 2:
            return (None,None)

        envelop_pos = 1
        while envelop_pos < len(frames):
            if len(frames[envelop_pos]) == 0:
                break
            envelop_pos += 1

        if envelop_pos == len(frames):
            return (frames[:1],frames[1:])
        elif envelop_pos + 1 < len(frames):
            return (frames[:envelop_pos + 1],frames[envelop_pos + 1:])
        else:
            return (frames,None)
    except Exception as e:
        logging.error('recv exception: {0}'.format(e))
        return (None,None)

_sym_db = _symbol_database.Default()

def get_pb_prototype(name):
    return _sym_db.GetSymbol(name)()


def pb_decode_frames(body):
    if body is not None and len(body) >= 2:
        msg_name = str(body[0])
        packet = body[1]
        if len(msg_name) > 0:
            msg = get_pb_prototype(msg_name)
            if msg is not None:
                if len(packet) > 0:
                    msg.ParseFromString(packet)
                    return msg
                elif msg.IsInitialized():
                    return msg
    return None

def pb_recv(sock,flags=0):
    envelope,body = zmsg_recv(sock,flags)
    return (envelope, pb_decode_frames(body))

def pb_router_recv(sock,flags=0):
    envelope,body = zmsg_router_recv(sock,flags)
    #if envelope is not None:
    #    logging.debug('zmsg recv envelop with %d frames' % len(envelope))
    #else:
    #    logging.debug('zmsg recv envelop none')

    #if body is not None:
    #    logging.debug('zmsg recv body with %d frames' % len(body))
    #else:
    #    logging.debug('zmsg recv body none')
    return (envelope, pb_decode_frames(body))


