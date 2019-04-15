# Real-Time Signalling

Hazaar has the ability to send and receive signals in real time to or from a client or server process.  This is done using a variety of techniques that depend on where the signal is coming from or going to.  For example: A server process will use UNIX sockets. A client browser will use WebSockets if supported otherwise it will use long-polling.

!!! notice

WebSockets can also be used if the client supports it.

See also: [Global Events](global-events.md)