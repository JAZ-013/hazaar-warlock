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

var HazaarWarlock = function (sid, host, useWebSockets, websocketsAutoReconnect, useSSL) {
    var o = this;
    this.encoded = false;
    this.sid = sid;
    this.host = host;
    this.useWebSockets = useWebSockets || true;
    this.websocketsAutoReconnect = websocketsAutoReconnect || true;
    this.ssl = useSSL || false;
    this.messageQueue = [];
    this.subscribeQueue = {};
    this.callbacks = {};
    this.connect = true;
    this.reconnectDelay = 0;
    this.reconnectRetries = 0;
    this.longPollingUrl = null;
    this.socket = null;
    this.sockets = [];
    this.username = null;
    this.admin_key = null;
    this._getGUID = function () {
        var guid = window.name;
        if (!guid) {
            this._log('Generating new GUID');
            guid = window.name = 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, function (c) {
                var r = Math.random() * 16 | 0, v = c == 'x' ? r : (r & 0x3 | 0x8);
                return v.toString(16);
            });
        }
        return guid;
    };
    this.connect = function () {
        if (!this._isWebSocket()) {
            this.longPollingUrl = 'http://' + this.host + '/warlock';
            return true;
        }
        if (!this.socket) {
            var url = 'ws' + (this.ssl ? 's' : '') + '://' + this.host + '/warlock?CID=' + this.guid;
            if (this.username) url += '&UID=' + btoa(this.username);
            this.socket = new WebSocket(url, 'warlock');
            try {
                this.socket.onopen = function (event) {
                    o.reconnectDelay = 0;
                    o.reconnectRetries = 0;
                    if (Object.keys(o.subscribeQueue).length > 0) {
                        for (event_id in o.subscribeQueue) {
                            o._subscribe(event_id, o.subscribeQueue[event_id].filter);
                        }
                    }
                    o._connectHandler(event);
                };
                this.socket.onmessage = function (event) {
                    return o._messageHandler(o._decode(event.data));
                };
                this.socket.onclose = function (event) {
                    delete o.socket;
                    return o._closeHandler(event);
                };
                this.socket.onerror = function (event) {
                    delete o.socket;
                    return o._errorHandler(event);
                }
            } catch (ex) {
                console.log(ex);
            }
        }
    };
    this._longpoll = function (event_id, callback, filter) {
        this._connectHandler();
        var packet = {
            'TYP': p.subscribe,
            'SID': this.sid,
            'PLD': {
                'id': event_id,
                'filter': filter
            }
        };
        var data = {
            CID: this.guid,
            P: this._encode(packet)
        };
        if (this.username) data['UID'] = btoa(this.username);
        var socket = $.get(this.longPollingUrl, data).done(function (data) {
            var result = true;
            o.reconnectRetries = 0;
            if (data.length > 0) {
                result = o._messageHandler(o._decode(data));
            }
            if (result) {
                setTimeout(function () {
                    o._longpoll(event_id, callback, filter);
                }, 0);
                return result;
            }
        }).fail(function (jqXHR) {
            o._closeHandler({ data: { 'id': event_id, 'callback': callback, 'filter': filter } });
        }).always(function () {
            o._unlongpoll();
        });
        this.sockets.push(socket);
        if (this.admin_key && this.sockets.length == 1) {
            this._send(p.sync, { 'admin_key': this.admin_key });
        }
    };
    this._unlongpoll = function (xhr) {
        for (x in o.sockets) {
            if (o.sockets[x].readyState == 4) {
                delete o.sockets[x];
            }
        }
    };
    this._isWebSocket = function () {
        return (this.useWebSockets && (("WebSocket" in window && window.WebSocket != undefined) || ("MozWebSocket" in window)))
    };
    this._disconnect = function () {
        this.connect = false;
        if (this._isWebSocket()) {
            this.socket.close();
        } else {
            for (i in this.sockets)
                this.sockets[i].abort();
            this.sockets = [];
        }
    };
    this._encode = function (packet) {
        packet = JSON.stringify(packet);
        return (this.encoded ? btoa(packet) : packet);
    };
    this._decode = function (packet) {
        if (packet.length == 0)
            return false;
        return JSON.parse((this.encoded ? atob(packet) : packet));
    };
    this._connectHandler = function (event) {
        if (this.messageQueue.length > 0) {
            for (i in this.messageQueue) {
                var msg = this.messageQueue[i];
                this._send(msg[0], msg[1]);
            }
            this.messageQueue = [];
        }
        if (this.callbacks.connect) this.callbacks.connect(event);
    };
    this._messageHandler = function (packet) {
        switch (packet.TYP) {
            case p.event:
                var event = packet.PLD;
                if (this.subscribeQueue[event.id]) this.subscribeQueue[event.id].callback(event.data, event);
                break;
            case p.error:
                if (this.callbacks.error) this.callbacks.error(packet.PLD);
                else alert('ERROR\n\nCommand:\t' + packet.PLD.command + '\n\nReason:\t\t' + packet.PLD.reason);
                return false;
                break;
            case p.status:
                if (this.callbacks.status) this.callbacks.status(packet.PLD);
                return true;
            case p.ping:
                this._send(p.pong);
                return true;
            case p.pong:
                if (this.callbacks.pong) this.callbacks.pong(packet.PLD);
                return true;
            case p.ok:
                return true;
            default:
                this._log('Protocol Error!');
                console.log(packet);
                this._disconnect();
                return false;
                break;
        }
        return true;
    };
    this._closeHandler = function (event) {
        if (this.connect) {
            this.reconnectRetries++;
            if (this.reconnectDelay < 30000)
                this.reconnectDelay = this.reconnectRetries * 1000;
            if (this._isWebSocket()) {
                if (this.websocketsAutoReconnect) {
                    setTimeout(function () {
                        o.connect();
                    }, this.reconnectDelay);
                }
            } else {
                setTimeout(function () {
                    o._longpoll(event.data.id, event.data.callback, event.data.filter);
                }, this.reconnectDelay);
            }
        } else {
            if (this.callbacks.close) o.callbacks.close(event);
            this.messageQueue = [];
            this.subscribeQueue = {};
        }
    };
    this._errorHandler = function (event) {
        if (o.callbacks.error) o.callbacks.error(event);
    };
    this._subscribe = function (event_id, filter) {
        this._send(p.subscribe, {
            'id': event_id,
            'filter': filter
        }, false);
    };
    this._unsubscribe = function (event_id) {
        this._send(p.unsubscribe, {
            'id': event_id
        }, false);
    };
    this._send = function (type, payload, queue) {
        var packet = {
            'TYP': type,
            'SID': this.sid,
            'TME': Math.round((new Date).getTime() / 1000)
        };
        if (typeof payload != 'undefined') packet.PLD = payload;
        if (this._isWebSocket()) {
            if (o.socket && o.socket.readyState == 1)
                this.socket.send(this._encode(packet));
            else if (queue)
                this.messageQueue.push([type, payload]);
        } else {
            packet.CID = this.guid;
            $.post(this.longPollingUrl, { CID: this.guid, P: this._encode(packet) }).done(function (data) {
                var packet = o._decode(data);
                if (packet.TYP == p.ok) {
                } else {
                    alert('An error occured sending the trigger event!');
                }
            }).fail(function (xhr) {
                o.messageQueue.push([type, payload]);
            });
        }
    };
    this._log = function (msg) {
        console.log('Warlock: ' + msg);
    };
    this.connected = function () {
        return (this.socket && this.socket.readyState == 1);
    };
    this.sync = function (admin_key) {
        this.admin_key = admin_key;
        this._send(p.sync, { 'admin_key': this.admin_key }, true);
        return this;
    };
    this.onconnect = function (callback) {
        this.callbacks.connect = callback;
        return this;
    };
    this.onclose = function (callback) {
        this.callbacks.close = callback;
        return this;
    };
    this.onerror = function (callback) {
        this.callbacks.error = callback;
        return this;
    };
    this.onstatus = function (callback) {
        this.callbacks.status = callback;
        return this;
    };
    this.onpong = function (callback) {
        this.callbacks.pong = callback;
        return this;
    };
    this.close = function () {
        this._disconnect();
        return this;
    };
    this.subscribe = function (event_id, callback, filter) {
        this.subscribeQueue[event_id] = {
            'callback': callback, 'filter': filter
        };
        if (this._isWebSocket()) {
            this._subscribe(event_id, filter);
        } else {
            this._longpoll(event_id, callback, filter);
        }
        return this;
    };
    this.unsubscribe = function (event_id) {
        if (!(this._isWebSocket() && this.subscribeQueue[event_id]))
            return false;
        delete this.subscribeQueue[event_id];
        this._unsubscribe(event_id);
        return this;
    };
    this.trigger = function (event_id, data, echo_self) {
        this._send(p.trigger, {
            'id': event_id,
            'data': data,
            'echo': (echo_self === true)
        }, true);
    };
    this.enable = function (service) {
        this._send(p.enable, service);
    };
    this.disable = function (service) {
        this._send(p.disable, service);
    };
    this.enableEncoding = function () {
        this.encoded = true;
    };
    this.enableWebsockets = function (autoReconnect) {
        this.useWebSockets = true;
        this.websocketsAutoReconnect = (typeof autoReconnect == 'undefined') ? true : autoReconnect;
    };
    this.setUser = function (username) {
        this.username = username;
    };
    this.status = function () {
        this._send(p.status);
    }
    this.spawn = function (service, params) {
        this._send(p.spawn, { 'name': service, 'params': params });
    };
    this.kill = function (service) {
        this._send(p.kill, { name: service });
    };
    this.signal = function (service, event_id, data) {
        this._send(p.signal, {
            'service': service,
            'id': event_id,
            'data': data
        }, true);
    };
    this.guid = this._getGUID();
    this._log('GUID=' + this.guid);
    this._log('Server ID=' + this.sid);
    return this;
};
