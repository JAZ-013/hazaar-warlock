var p = {
    noop: 0x00,         //Null Opperation
    sync: 0x01,         //Sync with server
    ok: 0x02,           //OK response
    error: 0x03,        //Error response
    status: 0x04,       //Status request/response
    shutdown: 0x05,     //Shutdown request
    delay: 0x06,        //Execute code after a period
    schedule: 0x07,     //Execute code at a set time
    cancel: 0x08,       //Cancel a pending code execution
    enable: 0x09,       //Start a service
    disable: 0x0A,      //Stop a service
    service: 0X0B,      //Service status
    subscribe: 0x0C,    //Subscribe to an event
    unsubscribe: 0x0D,  //Unsubscribe from an event
    trigger: 0x0E,      //Trigger an event
    event: 0x0F,        //An event
    exec: 0x10,         //Execute some code in the Warlock Runner.
    ping: 0x11,         //Typical PING
    pong: 0x12,         //Typical PONG
    log: 0x13,          //Send a log message to the server
    spawn: 0x14,        //Spawn a new dynamic service on this connection
    kill: 0x15,         //Kill a dynamic service
    signal: 0x16,       //Send a signal to a dynamic service
    debug: 0xFF
};

//var HazaarWarlock = function (sid, host, useWebSockets, websocketsAutoReconnect, useSSL) {
var HazaarWarlock = function (options) {
    var o = this;
    this.__options = hazaar.extend(options, {
        "sid": null,
        "connect": true,
        "server": "localhost",
        "port": 8000,
        "ssl": false,
        "websockets": true,
        "url": null,
        "check": true,
        "pingWait": 5000,
        "pingCount": 3,
        "reconnect": true,
        "reconnectDelay": 0,
        "reconnectRetries": 0,
        "username": null,
        "encoded": false
    });
    this.__longPollingUrl = null;
    this.__messageQueue = [];
    this.__subscribeQueue = {};
    this.__callbacks = {};
    this.__socket = null;
    this.__sockets = [];
    this.__admin_key = null;
    this.__connect = false;
    this.__getGUID = function () {
        var guid = window.name;
        if (!guid) {
            this.__log('Generating new GUID');
            guid = window.name = 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, function (c) {
                var r = Math.random() * 16 | 0, v = c == 'x' ? r : (r & 0x3 | 0x8);
                return v.toString(16);
            });
        }
        return guid;
    };
    this.connect = function () {
        this.__connect = true;
        if (!this.__isWebSocket()) {
            this.__longPollingUrl = 'http://' + this.__options.server
                + ':' + this.__options.port
                + '/' + this.__options.applicationName
                + '/warlock';
            return true;
        }
        if (!this.__socket) {
            var url = 'ws' + (this.__options.ssl ? 's' : '') + '://'
                + this.__options.server + ':'
                + this.__options.port + '/'
                + this.__options.applicationName
                + '/warlock?CID=' + this.guid;
            if (this.__options.username) url += '&UID=' + btoa(this.__options.username);
            this.__socket = new WebSocket(url, 'warlock');
            try {
                this.__socket.onopen = function (event) {
                    o.__options.reconnectDelay = 0;
                    o.__options.reconnectRetries = 0;
                    if (Object.keys(o.__subscribeQueue).length > 0) {
                        for (event_id in o.__subscribeQueue) {
                            o.__subscribe(event_id, o.__subscribeQueue[event_id].filter);
                        }
                    }
                    o.__connectHandler(event);
                };
                this.__socket.onmessage = function (event) {
                    return o.__messageHandler(o.__decode(event.data));
                };
                this.__socket.onclose = function (event) {
                    delete o.__socket;
                    return o.__closeHandler(event);
                };
                this.__socket.onerror = function (event) {
                    delete o.__socket;
                    return o.__errorHandler(event);
                }
            } catch (ex) {
                console.log(ex);
            }
        }
    };
    this.__longpoll = function (event_id, callback, filter) {
        this.__connectHandler();
        var packet = {
            'TYP': p.subscribe,
            'SID': this.__options.sid,
            'PLD': {
                'id': event_id,
                'filter': filter
            }
        };
        var data = {
            CID: this.guid,
            P: this.__encode(packet)
        };
        if (this.username) data['UID'] = btoa(this.username);
        var socket = $.get(this.__longPollingUrl, data).done(function (data) {
            var result = true;
            o.__options.reconnectRetries = 0;
            if (data.length > 0) {
                result = o.__messageHandler(o.__decode(data));
            }
            if (result) {
                setTimeout(function () {
                    o.__longpoll(event_id, callback, filter);
                }, 0);
                return result;
            }
        }).fail(function (jqXHR) {
            o.__closeHandler({ data: { 'id': event_id, 'callback': callback, 'filter': filter } });
        }).always(function () {
            o.__unlongpoll();
        });
        this.__sockets.push(socket);
        if (this.admin_key && this.__sockets.length == 1) {
            this.__send(p.sync, { 'admin_key': this.admin_key });
        }
    };
    this.__unlongpoll = function (xhr) {
        for (x in o.__sockets) {
            if (o.__sockets[x].readyState == 4)
                delete o.__sockets[x];
        }
    };
    this.__isWebSocket = function () {
        return (this.__options.websockets && (("WebSocket" in window && window.WebSocket != undefined) || ("MozWebSocket" in window)))
    };
    this.__disconnect = function () {
        this.__connect = false;
        if (this.__isWebSocket()) {
            this.__socket.close();
            this.__socket = null;
        } else {
            for (i in this.__sockets)
                this.__sockets[i].abort();
            this.__sockets = [];
        }
    };
    this.__encode = function (packet) {
        packet = JSON.stringify(packet);
        return (this.__options.encoded ? btoa(packet) : packet);
    };
    this.__decode = function (packet) {
        if (packet.length == 0)
            return false;
        return JSON.parse((this.__options.encoded ? atob(packet) : packet));
    };
    this.__connectHandler = function (event) {
        if (this.__messageQueue.length > 0) {
            for (i in this.__messageQueue) {
                var msg = this.__messageQueue[i];
                this.__send(msg[0], msg[1]);
            }
            this.__messageQueue = [];
        }
        if (this.__callbacks.connect) this.__callbacks.connect(event);
    };
    this.__messageHandler = function (packet) {
        switch (packet.TYP) {
            case p.event:
                var event = packet.PLD;
                if (this.__subscribeQueue[event.id]) this.__subscribeQueue[event.id].callback(event.data, event);
                break;
            case p.error:
                if (this.__callbacks.error) this.__callbacks.error(packet.PLD);
                else alert('ERROR\n\nCommand:\t' + packet.PLD.command + '\n\nReason:\t\t' + packet.PLD.reason);
                return false;
                break;
            case p.status:
                if (this.__callbacks.status) this.__callbacks.status(packet.PLD);
                return true;
            case p.ping:
                this.__send(p.pong);
                return true;
            case p.pong:
                if (this.__callbacks.pong) this.__callbacks.pong(packet.PLD);
                return true;
            case p.ok:
                return true;
            default:
                this.__log('Protocol Error!');
                console.log(packet);
                this.__disconnect();
                return false;
                break;
        }
        return true;
    };
    this.__closeHandler = function (event) {
        if (this.__connect) {
            this.__options.reconnectRetries++;
            if (this.__options.reconnectDelay < 30000)
                this.__options.reconnectDelay = this.__options.reconnectRetries * 1000;
            if (this.__isWebSocket()) {
                if (this.__options.reconnect) {
                    setTimeout(function () {
                        o.connect();
                    }, this.__options.reconnectDelay);
                }
            } else {
                setTimeout(function () {
                    o.__longpoll(event.data.id, event.data.callback, event.data.filter);
                }, this.__options.reconnectDelay);
            }
        } else {
            if (this.__callbacks.close) o.__callbacks.close(event);
            this.__messageQueue = [];
            this.__subscribeQueue = {};
        }
    };
    this.__errorHandler = function (event) {
        if (o.__callbacks.error) o.__callbacks.error(event);
    };
    this.__subscribe = function (event_id, filter) {
        this.__send(p.subscribe, {
            'id': event_id,
            'filter': filter
        }, false);
    };
    this.__unsubscribe = function (event_id) {
        this.__send(p.unsubscribe, {
            'id': event_id
        }, false);
    };
    this.__send = function (type, payload, queue) {
        var packet = {
            'TYP': type,
            'SID': this.__options.sid,
            'TME': Math.round((new Date).getTime() / 1000)
        };
        if (typeof payload != 'undefined') packet.PLD = payload;
        if (this.__isWebSocket()) {
            if (o.__socket && o.__socket.readyState == 1)
                this.__socket.send(this.__encode(packet));
            else if (queue)
                this.__messageQueue.push([type, payload]);
        } else {
            packet.CID = this.guid;
            $.post(this.__longPollingUrl, { CID: this.guid, P: this.__encode(packet) }).done(function (data) {
                var packet = o.__decode(data);
                if (packet.TYP == p.ok) {
                } else {
                    alert('An error occured sending the trigger event!');
                }
            }).fail(function (xhr) {
                o.__messageQueue.push([type, payload]);
            });
        }
    };
    this.__log = function (msg) {
        console.log('Warlock: ' + msg);
    };
    this.connected = function () {
        return (this.__socket && this.__socket.readyState == 1);
    };
    this.sync = function (admin_key) {
        this.admin_key = admin_key;
        this.__send(p.sync, { 'admin_key': this.admin_key }, true);
        return this;
    };
    this.onconnect = function (callback) {
        this.__callbacks.connect = callback;
        return this;
    };
    this.onclose = function (callback) {
        this.__callbacks.close = callback;
        return this;
    };
    this.onerror = function (callback) {
        this.__callbacks.error = callback;
        return this;
    };
    this.onstatus = function (callback) {
        this.__callbacks.status = callback;
        return this;
    };
    this.onpong = function (callback) {
        this.__callbacks.pong = callback;
        return this;
    };
    this.close = function () {
        this.__disconnect();
        return this;
    };
    this.subscribe = function (event_id, callback, filter) {
        this.__subscribeQueue[event_id] = {
            'callback': callback, 'filter': filter
        };
        if (this.__isWebSocket()) {
            this.__subscribe(event_id, filter);
        } else {
            this.__longpoll(event_id, callback, filter);
        }
        return this;
    };
    this.unsubscribe = function (event_id) {
        if (!(this.__isWebSocket() && this.__subscribeQueue[event_id]))
            return false;
        delete this.__subscribeQueue[event_id];
        this.__unsubscribe(event_id);
        return this;
    };
    this.trigger = function (event_id, data, echo_self) {
        this.__send(p.trigger, {
            'id': event_id,
            'data': data,
            'echo': (echo_self === true)
        }, true);
        return this;
    };
    this.enable = function (service) {
        this.__send(p.enable, service);
        return this;
    };
    this.disable = function (service) {
        this.__send(p.disable, service);
        return this;
    };
    this.enableEncoding = function () {
        this.__options.encoded = true;
        return this;
    };
    this.enableWebsockets = function (autoReconnect) {
        this.__options.websockets = true;
        this.__options.reconnect = (typeof autoReconnect == 'undefined') ? true : autoReconnect;
        return this;
    };
    this.setUser = function (username) {
        this.username = username;
        return this;
    };
    this.status = function () {
        this.__send(p.status);
        return this;
    }
    this.spawn = function (service, params) {
        this.__send(p.spawn, { 'name': service, 'params': params });
        return this;
    };
    this.kill = function (service) {
        this.__send(p.kill, { name: service });
        return this;
    };
    this.signal = function (service, event_id, data) {
        this.__send(p.signal, {
            'service': service,
            'id': event_id,
            'data': data
        }, true);
        return this;
    };
    this.guid = this.__getGUID();
    this.__log('GUID=' + this.guid);
    this.__log('Server ID=' + this.__options.sid);
    if (this.__options.connect === true) this.connect();
    return this;
};
