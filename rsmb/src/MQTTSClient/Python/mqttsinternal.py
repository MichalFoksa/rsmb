"""
/*******************************************************************************
 * Copyright (c) 2011, 2013 IBM Corp.
 *
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * and Eclipse Distribution License v1.0 which accompany this distribution. 
 *
 * The Eclipse Public License is available at 
 *    http://www.eclipse.org/legal/epl-v10.html
 * and the Eclipse Distribution License is available at 
 *   http://www.eclipse.org/org/documents/edl-v10.php.
 *
 * Contributors:
 *    Ian Craggs - initial API and implementation and/or initial documentation
 *******************************************************************************/
"""

import MQTTs, time, sys, socket, traceback

debug = False

class Receivers:

  def __init__(self, socket):
    print "initializing receiver"
    self.socket = socket
    self.connected = False
    self.observe = None
    self.observed = []

    self.inMsgs = {}
    self.outMsgs = {}

    self.puback = MQTTs.Pubacks()
    self.pubrec = MQTTs.Pubrecs()
    self.pubrel = MQTTs.Pubrels()
    self.pubcomp = MQTTs.Pubcomps()

  def lookfor(self, msgType):
    self.observe = msgType

  def waitfor(self, msgType, msgId=None):
    msg = None
    count = 0
    while True:
      while len(self.observed) > 0:
        msg = self.observed.pop(0)
        if msg.mh.MsgType == msgType and (msgId == None or msg.MsgId == msgId):
          break
        else:
          msg = None
      if msg != None:
        break
      time.sleep(0.2)
      count += 1
      if count == 25:
        msg = None
        break
    self.observe = None
    return msg

  def receive(self, callback=None):
    packet = None
    try:
      packet, address = MQTTs.unpackPacket(MQTTs.getPacket(self.socket))
    except:
      if sys.exc_info()[0] != socket.timeout:
        print "unexpected exception", sys.exc_info()
        raise sys.exc_info()
    if packet == None:
      time.sleep(0.1)
      return
    elif debug:
      print packet

    if self.observe == packet.mh.MsgType:
      print "observed", packet
      self.observed.append(packet)
        
    elif packet.mh.MsgType == MQTTs.ADVERTISE:
      if hasattr(callback, "advertise"):
        callback.advertise(address, packet.GwId, packet.Duration)

    elif packet.mh.MsgType == MQTTs.REGISTER:
      if callback and hasattr(callback, "register"):
        callback.register(packet.TopicId, packet.Topicname)

    elif packet.mh.MsgType == MQTTs.PUBACK:
      "check if we are expecting a puback"
      if self.outMsgs.has_key(packet.MsgId) and \
        self.outMsgs[packet.MsgId].Flags.QoS == 1:
        del self.outMsgs[packet.MsgId]
        if hasattr(callback, "published"):
          callback.published(packet.MsgId)
      else:
        raise Exception("No QoS 1 message with message id "+str(packet.MsgId)+" sent")

    elif packet.mh.MsgType == MQTTs.PUBREC:
      if self.outMsgs.has_key(packet.MsgId):
        self.pubrel.MsgId = packet.MsgId
        self.socket.send(self.pubrel.pack())
      else:
        raise Exception("PUBREC received for unknown msg id "+ \
                    str(packet.MsgId))

    elif packet.mh.MsgType == MQTTs.PUBREL:
      "release QOS 2 publication to client, & send PUBCOMP"
      msgid = packet.MsgId
      if not self.inMsgs.has_key(msgid):
        pass # what should we do here?
      else:
        pub = self.inMsgs[packet.MsgId]
        if callback == None or \
           callback.messageArrived(pub.TopicName, pub.Data, 2, pub.Flags.Retain, pub.MsgId):
          del self.inMsgs[packet.MsgId]
          self.pubcomp.MsgId = packet.MsgId
          self.socket.send(self.pubcomp.pack())
        if callback == None:
          return (pub.TopicName, pub.Data, 2, pub.Flags.Retain, pub.MsgId)

    elif packet.mh.MsgType == MQTTs.PUBCOMP:
      "finished with this message id"
      if self.outMsgs.has_key(packet.MsgId):
        del self.outMsgs[packet.MsgId]
        if hasattr(callback, "published"):
          callback.published(packet.MsgId)
      else:
        raise Exception("PUBCOMP received for unknown msg id "+ \
                    str(packet.MsgId))

    elif packet.mh.MsgType == MQTTs.PUBLISH:
      "finished with this message id"
      if packet.Flags.QoS in [0, 3]:
        qos = packet.Flags.QoS
        topicname = packet.TopicName
        data = packet.Data
        if qos == 3:
          qos = -1
          if packet.Flags.TopicIdType == MQTTs.TOPICID:
            topicname = packet.Data[:packet.TopicId]
            data = packet.Data[packet.TopicId:]
        if callback == None:
          return (topicname, data, qos, packet.Flags.Retain, packet.MsgId)
        else:
          callback.messageArrived(topicname, data, qos, packet.Flags.Retain, packet.MsgId)
      elif packet.Flags.QoS == 1:
        if callback == None:
          return (packet.topicName, packet.Data, 1,
                           packet.Flags.Retain, packet.MsgId)
        else:
          if callback.messageArrived(packet.TopicName, packet.Data, 1,
                           packet.Flags.Retain, packet.MsgId):
            self.puback.MsgId = packet.MsgId
            self.socket.send(self.puback.pack())
      elif packet.Flags.QoS == 2:
        self.inMsgs[packet.MsgId] = packet
        self.pubrec.MsgId = packet.MsgId
        self.socket.send(self.pubrec.pack())

    else:
      raise Exception("Unexpected packet"+str(packet))
    return packet

  def __call__(self, callback):
    try:
      while True:
        self.receive(callback)
    except:
      if sys.exc_info()[0] != socket.error:
        print "unexpected exception", sys.exc_info()
        traceback.print_exc()
