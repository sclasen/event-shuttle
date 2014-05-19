event-shuttle
=============

goal: unix system service that collects transactional events and reliably delivers them to the event processing infrastructure,
 relieving other services on the same system from having to do so.

listens on 127.0.0.1:3887, rest-api `POST /:channel -d envelope.json` endpoint for events from github.com/heroku/events.

journal to bolt db

talk to ringmaster, megaphone.


