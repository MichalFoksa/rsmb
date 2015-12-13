Change log
----------
* 15-May-2015: Added support of **Forwarder Encapsulation**. Mqtt-SN packets are encapsulated according to MQTT-SN Protocol Specification v1.2, chapter 5.5 Forwarder Encapsulation.
* 01-Apr-2015: Pre-defined topic name is stripped of trailing white space characters
* 22-Mar-2015: Added support for **Pre-defined topics** and **Dynamic pre-defined topic** names.
* 22-Mar-2015: In MQTT-SN, PubAck send with "Rejected - Invalid topic ID" when topic name not
    found at message publish.
* 22-Mar-2015: Fixed Eclipse bug [424704](https://bugs.eclipse.org/bugs/show_bug.cgi?id=424704) - MQTT-SN broker forgets published topics.

For full list of changes see [CHANGELOG](./CHANGELOG "CHANGELOG") file.

Overview
--------
When I was looking for a messaging platform which I could use in home automation project I discovered MQTT-SN (MQTT for Sensor Networks), even lighter then its already lightweight sibling MQTT ([MQ Telemetry Transport](http://mqtt.org/)), protocol.

While there are few available MQTT brokers, it seems there is only one MQTT-SN broker called **Really Small Message Broker**, or **RSMB**, so my choice was easy.

After I experienced with RSMB I discovered couple of missing features (e.g: incomplete support of sleeping clients, pre-defined topic IDs) of MQTT-SN protocol. While MQTT features of RSMB are already incorporated into its successor [Mosquitto](http://mosquitto.org/) there is not much buzz around MQTT-SN and my personal opinion is that it is considered secondary.

Fortunately RSMB has been open sourced and its code is available on [Eclipse.org](http://git.eclipse.org/c/mosquitto/org.eclipse.mosquitto.rsmb.git) so I decided help myself, fix some bugs, add few features and do it Open.

Hopefully couple of you will find it useful and will benefit from it.

My expectations from the project
------------
Fix few bugs, add some features so the MQTT-SN support in RSMB will be good enough small applications.


More on MQTT, MQTT-SN and RSMB
------------------------------
[MQTT-SN specification](http://mqtt.org/new/wp-content/uploads/2009/06/MQTT-SN_spec_v1.2.pdf), version 1.2. This is a document I mostly work with when figuring out how particular feature should work.

More on RSMB is in [Getting Started](https://rawgit.com/MichalFoksa/rsmb/master/rsmb/doc/gettingstarted.htm). It covers [protocol concepts](https://rawgit.com/MichalFoksa/rsmb/master/rsmb/doc/gettingstarted.htm#basics), RSMB [configuration](https://rawgit.com/MichalFoksa/rsmb/master/rsmb/doc/gettingstarted.htm#configfiles), [troubleshooting](https://rawgit.com/MichalFoksa/rsmb/master/rsmb/doc/gettingstarted.htm#troubleshooting), [bridging](https://rawgit.com/MichalFoksa/rsmb/master/rsmb/doc/gettingstarted.htm#bridging) two brokers together, etc.

Since beginning of 2015 HiveMq is running series of informative blogs [MQTT Essentials](http://www.hivemq.com/blog/).


Credits
-------
Most of credits belong to Ian Craggs and Nicholas O'Leary from IBM UK - original makers of RSMB.

And of course other small contributors who gave hand to the project.


License
-------
[Eclipse Public License Version 1.0](https://www.eclipse.org/legal/epl-v10.html)


[![Analytics](https://ga-beacon.appspot.com/UA-57939436-3/RSMB/README?pixel)](https://github.com/igrigorik/ga-beacon)
