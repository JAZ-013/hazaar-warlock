var p = {
    //SYSTEM MESSAGES
    noop: 0x00,         //Null Opperation
    sync: 0x01,         //Sync client
    ok: 0x02,           //OK response
    error: 0x03,        //Error response
    status: 0x04,       //Status request/response
    shutdown: 0x05,     //Shutdown request
    ping: 0x06,         //Typical PING
    pong: 0x07,         //Typical PONG

    //CODE EXECUTION MESSAGES
    delay: 0x10,        //Execute code after a period
    schedule: 0x11,     //Execute code at a set time
    exec: 0x12,         //Execute some code in the Warlock Runner.
    cancel: 0x13,       //Cancel a pending code execution

    //SIGNALLING MESSAGES
    subscribe: 0x20,    //Subscribe to an event
    unsubscribe: 0x21,  //Unsubscribe from an event
    trigger: 0x22,      //Trigger an event
    event: 0x23,        //An event

    //SERVICE MESSAGES
    enable: 0x30,       //Start a service
    disable: 0x31,      //Stop a service
    service: 0x32,      //Service status
    spawn: 0x33,        //Spawn a dynamic service
    kill: 0x34,         //Kill a dynamic service instance
    signal: 0x35,       //Send a trigger signal directly to an attached service

    //LOGGING/OUTPUT MESSAGES
    log: 0x90,          //Generic log message
    debug: 0x91
};

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
                var r = Math.random() * 16 | 0, v = c === 'x' ? r : (r & 0x3 | 0x8);
                return v.toString(16);
            });
        }
        return guid;
    };
    this.connect = function () {
        this.__connect = true;
        if (!this.__socket) {
            var url = 'ws' + (this.__options.ssl ? 's' : '') + '://'
                + (this.__options.username ? btoa(this.__options.username) + '@' : '')
                + this.__options.server + ':'
                + this.__options.port + '/'
                + this.__options.applicationName
                + '/warlock?CID=' + this.guid;
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
                };
            } catch (ex) {
                console.log(ex);
            }
        }
    };
    this.__disconnect = function () {
        this.__connect = false;
        this.__socket.close();
        this.__socket = null;
    };
    this.__encode = function (packet) {
        packet = JSON.stringify(packet);
        return (this.__options.encoded ? btoa(packet) : packet);
    };
    this.__decode = function (packet) {
        if (packet.length === 0)
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
                else console.error('Command: ' + packet.PLD.command + ' Reason: ' + packet.PLD.reason);
                return false;
            case p.status:
                if (this.__callbacks.status) this.__callbacks.status(packet.PLD);
                break;
            case p.ping:
                this.__send(p.pong);
                break;
            case p.pong:
                if (this.__callbacks.pong) this.__callbacks.pong(packet.PLD);
                break;
            case p.ok:
                break;
            default:
                console.log(packet.PLD);
                break;
        }
        return true;
    };
    this.__closeHandler = function (event) {
        if (this.__connect) {
            this.__options.reconnectRetries++;
            if (this.__options.reconnectDelay < 30000)
                this.__options.reconnectDelay = this.__options.reconnectRetries * 1000;
            if (this.__options.reconnect) {
                setTimeout(function () {
                    o.connect();
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
        if (typeof payload !== 'undefined') packet.PLD = payload;
        if (o.__socket && o.__socket.readyState === 1)
            this.__socket.send(this.__encode(packet));
        else if (queue)
            this.__messageQueue.push([type, payload]);
    };
    this.__log = function (msg) {
        console.log('Warlock: ' + msg);
    };
    this.connected = function () {
        return (this.__socket && this.__socket.readyState === 1);
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
    /* Client Commands */
    this.sync = function (admin_key) {
        this.admin_key = admin_key;
        this.__send(p.sync, { 'admin_key': this.admin_key }, true);
        return this;
    };
    this.subscribe = function (event_id, callback, filter) {
        this.__subscribeQueue[event_id] = {
            'callback': callback, 'filter': filter
        };
        this.__subscribe(event_id, filter);
        return this;
    };
    this.unsubscribe = function (event_id) {
        if (!this.__subscribeQueue[event_id])
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
    /* Admin Commands (These require sync with admin key) */
    this.stop = function (delay_sec) {
        this.__send(p.shutdown, (delay_sec > 0 ? { delay: delay_sec } : null));
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
    this.service = function (name) {
        this.__send(p.service, name);
        return this;
    };
    this.enableEncoding = function () {
        this.__options.encoded = true;
        return this;
    };
    this.status = function () {
        this.__send(p.status);
        return this;
    };
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
