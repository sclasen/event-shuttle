event-shuttle
=============

goal: unix system service that collects transactional events and reliably delivers them to the event processing infrastructure,
 relieving other services on the same system from having to do so.

provides a normalized interface to the event processing infrastructure, so that we can change the underlying implementation without app changes.

journals events through bolt-db so that in the event of an event processing infrastructure outage, events can still be accepted, and will be delivered when the infrastructre becomes available.

* listens on 127.0.0.1:3887, rest-api is `POST /:channel -d envelope.json`
* validates the envelope and the event it contains against schema for events from `github.com/heroku/events`.
* journals events to bolt db.
* discovers brokers via ringmaster.
* delivers events to megaphone.

status
------

* everything above except event json validation.




