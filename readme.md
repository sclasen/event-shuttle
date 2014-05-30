event-shuttle
=============

goal: unix system service that collects events and reliably delivers them to kafka,
 relieving other services on the same system from having to do so.

journals events through bolt-db so that in the event of an kafka outage, events can still be accepted, and will be delivered when kafka becomes available.

* listens on 127.0.0.1:3887, rest-api is `POST /:topic -d message`
* journals events to bolt db.
* discovers brokers via netflix exhibitor, or a seed broker list.
* delivers events to kafka.






