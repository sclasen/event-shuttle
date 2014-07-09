event-shuttle
=============

goal: unix system service that collects events and reliably delivers them to kafka,
 relieving other services on the same system from having to do so.

journals events through bolt-db so that in the event of an kafka outage, events can still be accepted, and will be delivered when kafka becomes available.

* listens on 127.0.0.1:3887, rest-api is `POST /:topic -d message`
* journals events to bolt db.
* discovers brokers via netflix exhibitor, or a seed broker list.
* delivers events to kafka.


```
 event-shuttle --help
Usage of event-shuttle:
  -brokers="": comma seperated list of ip:port to use as seed brokers
  -db="events.db": name of the boltdb database file
  -debug=false: start a pprof http server on 6060
  -exhibitor=false: use EXHIBITOR_URL from env to lookup seed brokers
  -port="3887": port on which to listen for events
```

using a broker list
-------------------

`event-shuttle -brokers 1.2.3.4:9092,1.2.3.5:9092`

using netflix exhibitor to find brokers
---------------------------------------

`EXHIBITOR_URL=<some httpsUrl with creds> event-shuttle -exhibitor=true`









