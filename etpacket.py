#!/usr/bin/python

import logging
import struct
from google.protobuf import descriptor_pool as _descpool
from google.protobuf import descriptor_pb2 as _descriptor_pb2
from google.protobuf import reflection as _reflection
from google.protobuf import message_factory
from google.protobuf import symbol_database as _symbol_database

import ptt.ptt_pb2
import ptt.net_pb2
import ptt.rr_pb2
import ptt.push_pb2
import ptt.loc_pb2


_sym_db = _symbol_database.Default()

#file_descriptors = [ ptt_pb2.DESCRIPTOR,net_pb2.DESCRIPTOR,rr_pb2.DESCRIPTOR,push_pb2.DESCRIPTOR,loc_pb2.DESCRIPTOR] 
#message_classes = message_factory.GetMessages(file_descriptors)

#my_proto_instance = message_classes['some.proto.package.MessageName']()


def get_prototype(name):
    return _sym_db.GetSymbol(name)()


def pack(protomsg):
    namelen = len(protomsg.DESCRIPTOR.full_name) + 1
    protoraw = protomsg.SerializeToString()
    packetlen = 2 + namelen + len(protoraw) + 4
    fmt = "!IH%ds%dsI" % (namelen,len(protoraw))

    packet = struct.pack(fmt,packetlen,namelen,protomsg.DESCRIPTOR.full_name,protoraw,0)
    return packet

def unpack(packet):
    if len(packet) < 6:
        return (0,None)

    packetlen,namelen = struct.unpack("!IH",packet[:6])
    if packetlen > len(packet) - 4:
        return (packetlen,None)

    if namelen <= 0 or namelen > packetlen - 4 - 2:
        return (-1,None)

    msglen = packetlen - 2 - 4 - namelen
    fmt = "!IH%dsx%dsI" % (namelen - 1,msglen)
    packetlen,namelen,name,msgraw,crc = struct.unpack(fmt,packet[:packetlen+4])

    prototype = get_prototype(name)

    prototype.ParseFromString(msgraw)
    return (packetlen+4,prototype)

def rtp_pack(paytype,seq,ts,ssrc,pay):
    fmt = '!BBHII%ds' % (len(pay))
    rtpVer = 2 << 6
    return struct.pack(fmt,rtpVer,paytype,seq,ts,ssrc,pay)

# return (paytype,seq,ts,ssrc,pay)
def rtp_unpack(packet):
    if len(packet) <= 12:
        logging.warning('packet too small: %d' % len(packet))
        return (None,None,None,None,None)
    fmt = '!BBHII'
    header,paytype,seq,ts,ssrc = struct.unpack(fmt,packet[:12])
    return (paytype,seq,ts,ssrc,packet[12:])



