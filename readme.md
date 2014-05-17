event-shuttle
=============

goal: unix system service that collects transactional events and reliably delivers them to the event processing infrastructure,
 relieving other services on the same system from having to do so.

listens on unix socket, rest-api `POST /:topic -d envelope.json` endpoint for events from github.com/heroku/events.

journal to a memory mapped file as circular buffer.

talk to ringmaster, megaphone.


