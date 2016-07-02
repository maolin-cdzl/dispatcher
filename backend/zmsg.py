# -*- coding: UTF-8 -*-

import logging
import zmq
from google.protobuf import symbol_database as _symbol_database
import disp_pb2

def zmsg_recv(sock):
    try:
        frames = sock.recv_multipart(zmq.NOBLOCK)
        if frames is None or len(frames) == 0:
            return (None,None)

        envelop_pos = 0
        while envelop_pos < len(frames):
            if len(frames) == 0:
                break
            envelop_pos += 1

        if envelop_pos == len(frames):
            return (None,frames)
        elif envelop_pos + 1 < len(frames):
            return (frames[:envelop_pos + 1],frames[envelop_pos + 1:])
        else:
            return (frames,None)
    except zmq.Again:
        return (None,None)

def zmsg_router_recv(sock):
    try:
        frames = sock.recv_multipart(zmq.NOBLOCK)
        if frames is None or len(frames) < 2:
            return (None,None)

        envelop_pos = 1
        while envelop_pos < len(frames):
            if len(frames) == 0:
                break
            envelop_pos += 1

        if envelop_pos == len(frames):
            return (frames[:1],frames[1:])
        elif envelop_pos + 1 < len(frames):
            return (frames[:envelop_pos + 1],frames[envelop_pos + 1:])
        else:
            return (frames,None)
    except zmq.Again:
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

def pb_recv(sock):
    envelope,body = zmsg_recv(sock)
    return (envelope, pb_decode_frames(body))

def pb_router_recv(sock):
    envelope,body = zmsg_router_recv(sock)
    return (envelope, pb_decode_frames(body))


