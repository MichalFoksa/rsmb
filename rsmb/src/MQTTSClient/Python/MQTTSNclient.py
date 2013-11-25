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

import MQTTSN, socket, time, MQTTSNinternal, thread, types, sys, struct


class Callback:

  def __init__(self):
    self.events = []
    self.registered = {}

  def connectionLost(self, cause):
    print "default connectionLost", cause
    self.events.append("disconnected")

  def messageArrived(self, topicName, payload, qos, retained, msgid):
    print "default publishArrived", topicName, payload, qos, retained, msgid
    return True

  def deliveryComplete(self, msgid):
    print "default deliveryComplete"
  
  def advertise(self, address, gwid, duration):
    print "advertise", address, gwid, duration

  def register(self, topicid, topicName):
    self.registered[topicId] = topicName


class Client:

  def __init__(self, clientid, host="localhost", port=1883):
    self.clientid = clientid
    self.host = host
    self.port = port
    self.msgid = 1
    self.callback = None
    self.__receiver = None
    
  def start(self):
    self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
    self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    self.sock.bind((self.host, self.port))
    mreq = struct.pack("4sl", socket.inet_aton(self.host), socket.INADDR_ANY)

    self.sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)
    
    self.startReceiver()
      
  def stop(self):
    self.stopReceiver()

  def __nextMsgid(self):
    def getWrappedMsgid():
      id = self.msgid + 1
      if id == 65535:
        id = 1
      return id

    if len(self.__receiver.outMsgs) >= 65535:
      raise "No slots left!!"
    else:
      self.msgid = getWrappedMsgid()
      while self.__receiver.outMsgs.has_key(self.msgid):
        self.msgid = getWrappedMsgid()
    return self.msgid


  def registerCallback(self, callback):
    self.callback = callback


  def connect(self, cleansession=True):
    self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    #self.sock.settimeout(5.0)

    self.sock.connect((self.host, self.port))

    connect = MQTTSN.Connects()
    connect.ClientId = self.clientid
    connect.CleanSession = cleansession
    connect.KeepAliveTimer = 0
    self.sock.send(connect.pack())

    response, address = MQTTSN.unpackPacket(MQTTSN.getPacket(self.sock))
    assert response.mh.MsgType == MQTTSN.CONNACK
    
    self.startReceiver()

    
  def startReceiver(self):
    self.__receiver = MQTTSNinternal.Receivers(self.sock)
    if self.callback:
      id = thread.start_new_thread(self.__receiver, (self.callback,))


  def waitfor(self, msgType, msgId=None):
    if self.__receiver:
      msg = self.__receiver.waitfor(msgType, msgId)
    else:
      msg = self.__receiver.receive()
      while msg.mh.MsgType != msgType and (msgId == None or msgId == msg.MsgId):
        msg = self.__receiver.receive()
    return msg


  def subscribe(self, topic, qos=2):
    subscribe = MQTTSN.Subscribes()
    subscribe.MsgId = self.__nextMsgid()
    if type(topic) == types.StringType:
      subscribe.TopicName = topic
      if len(topic) > 2:
        subscribe.Flags.TopicIdType = MQTTSN.TOPIC_NORMAL
      else:
        subscribe.Flags.TopicIdType = MQTTSN.TOPIC_SHORTNAME
    else:
      subscribe.TopicId = topic # should be int
      subscribe.Flags.TopicIdType = MQTTSN.TOPIC_PREDEFINED
    subscribe.Flags.QoS = qos
    if self.__receiver:
      self.__receiver.lookfor(MQTTSN.SUBACK)
    self.sock.send(subscribe.pack())
    msg = self.waitfor(MQTTSN.SUBACK, subscribe.MsgId)
    return msg.ReturnCode, msg.TopicId


  def unsubscribe(self, topics):
    unsubscribe = MQTTSN.Unsubscribes()
    unsubscribe.MsgId = self.__nextMsgid()
    unsubscribe.data = topics
    if self.__receiver:
      self.__receiver.lookfor(MQTTSN.UNSUBACK)
    self.sock.send(unsubscribe.pack())
    msg = self.waitfor(MQTTSN.UNSUBACK, unsubscribe.MsgId)
  
  
  def register(self, topicName):
    register = MQTTSN.Registers()
    register.TopicName = topicName
    if self.__receiver:
      self.__receiver.lookfor(MQTTSN.REGACK)
    self.sock.send(register.pack())
    msg = self.waitfor(MQTTSN.REGACK, register.MsgId)
    return msg.TopicId


  def publish(self, topic, payload, qos=0, retained=False):
    publish = MQTTSN.Publishes()
    publish.Flags.QoS = qos
    publish.Flags.Retain = retained
    if type(topic) == types.StringType:
      publish.Flags.TopicIdType = MQTTSN.TOPIC_SHORTNAME
      publish.TopicName = topic
    else:
      publish.Flags.TopicIdType = MQTTSN.TOPIC_NORMAL
      publish.TopicId = topic
    if qos in [-1, 0]:
      publish.MsgId = 0
    else:
      publish.MsgId = self.__nextMsgid()
      print "MsgId", publish.MsgId
      self.__receiver.outMsgs[publish.MsgId] = publish
    publish.Data = payload
    self.sock.send(publish.pack())
    return publish.MsgId
  

  def disconnect(self):
    disconnect = MQTTSN.Disconnects()
    if self.__receiver:
      self.__receiver.lookfor(MQTTSN.DISCONNECT)
    self.sock.send(disconnect.pack())
    msg = self.waitfor(MQTTSN.DISCONNECT)
    

  def stopReceiver(self):
    self.sock.close() # this will stop the receiver too
    assert self.__receiver.inMsgs == {}
    assert self.__receiver.outMsgs == {}
    self.__receiver = None

  def receive(self):
    return self.__receiver.receive()


def publish(topic, payload, retained=False, port=1883, host="localhost"):
  publish = MQTTSN.Publishes()
  publish.Flags.QoS = 3
  publish.Flags.Retain = retained  
  if type(topic) == types.StringType:
    if len(topic) > 2:
      publish.Flags.TopicIdType = MQTTSN.TOPIC_NORMAL
      publish.TopicId = len(topic)
      payload = topic + payload
    else:
      publish.Flags.TopicIdType = MQTTSN.TOPIC_SHORTNAME
      publish.TopicName = topic
  else:
    publish.Flags.TopicIdType = MQTTSN.TOPIC_NORMAL
    publish.TopicId = topic
  publish.MsgId = 0
  print "payload", payload
  publish.Data = payload
  sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
  sock.sendto(publish.pack(), (host, port))
  sock.close()
  return 


if __name__ == "__main__":
  
	"""
  mclient = Client("myclientid", host="225.0.18.83", port=1883)
  mclient.registerCallback(Callback())
  mclient.start()
  
  publish("long topic name", "qos -1 start", port=1884)

  callback = Callback()

  aclient = Client("myclientid", port=1884)
  aclient.registerCallback(callback)

  aclient.connect()
  aclient.disconnect()

  aclient.connect()
  aclient.subscribe("k ", 2)
  aclient.subscribe("jkjkjkjkj", 2)
  aclient.publish("k ", "qos 0")
  aclient.publish("k ", "qos 1", 1)
  aclient.publish("jkjkjkjkj", "qos 2", 2)
  topicid = aclient.register("jkjkjkjkj")
  #time.sleep(1.0)
  aclient.publish(topicid, "qos 2 - registered topic id", 2)
  #time.sleep(1.0)
  aclient.disconnect()
  publish("long topic name", "qos -1 end", port=1884)
  
  time.sleep(30)
  mclient.stop()
	"""


	aclient = Client("linh", port=1885)
	aclient.registerCallback(Callback())
	aclient.connect()

	rc, topic1 = aclient.subscribe("topic1")
	print "topic id for topic1 is", topic1
	rc, topic2 = aclient.subscribe("topic2")
	print "topic id for topic2 is", topic2
	aclient.publish(topic1, "aaaa", qos=0)
	aclient.publish(topic2, "bbbb", qos=0)
	aclient.unsubscribe("topic1")
	aclient.publish(topic2, "bbbb", qos=0)
	aclient.publish(topic1, "aaaa", qos=0)
	aclient.disconnect()



