var HazaarWarlock = function (options) {
    var o = this;
    this.p = null; //The current protocol
    this.op = null; //The original protocol as it was sent
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
                var r = Math.random() * 16 | 0, v = c === 'x' ? r : r & 0x3 | 0x8;
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
        return this.__options.encoded ? btoa(packet) : packet;
    };
    this.__decode = function (packet) {
        if (packet.length === 0)
            return false;
        return JSON.parse(this.__options.encoded ? atob(packet) : packet);
    };
    this.__connectHandler = function (event) {
        if (this.__callbacks.connect) this.__callbacks.connect(event);
    };
    this.__messageHandler = function (packet) {
        if (typeof packet === 'object' && '0' in packet && packet['0'] === 'NOOP') { //Initial packet
            this.op = packet;
            this.p = {};
            for (x in packet) this.p[packet[x].toLowerCase()] = parseInt(x);
            if (Object.keys(o.__subscribeQueue).length > 0)
                for (event_id in o.__subscribeQueue) o.__subscribe(event_id, o.__subscribeQueue[event_id].filter);
            if (this.__messageQueue.length > 0) {
                for (i in this.__messageQueue) {
                    var msg = this.__messageQueue[i];
                    this.__send(msg[0], msg[1]);
                }
                this.__messageQueue = [];
            }
        } else if (packet.TYP in this.op && this.op[packet.TYP].substr(0, 2) === 'kv') {
            var type = this.op[packet.TYP].toLowerCase();
            if (type in this.__callbacks && Array.isArray(this.__callbacks[type])) {
                let callback = this.__callbacks[type].shift();
                if (typeof callback === 'function') callback(packet.PLD);
            }
        } else {
            switch (packet.TYP) {
                case this.p.event:
                    var event = packet.PLD;
                    if (this.__subscribeQueue[event.id]) this.__subscribeQueue[event.id].callback(event.data, event);
                    break;
                case this.p.error:
                    if (this.__callbacks.error) this.__callbacks.error(packet.PLD);
                    else console.error('Command: ' + packet.PLD.command + ' Reason: ' + packet.PLD.reason);
                    return false;
                case this.p.status:
                    if (this.__callbacks.status) this.__callbacks.status(packet.PLD);
                    break;
                case this.p.ping:
                    this.__send('pong');
                    break;
                case this.p.pong:
                    if (this.__callbacks.pong) this.__callbacks.pong(packet.PLD);
                    break;
                case this.p.ok:
                    break;
                default:
                    console.log(packet.PLD);
                    break;
            }
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
        this.__send('subscribe', {
            'id': event_id,
            'filter': filter
        }, false);
    };
    this.__unsubscribe = function (event_id) {
        this.__send('unsubscribe', {
            'id': event_id
        }, false);
    };
    this.__send = function (type, payload, queue) {
        if (this.__socket && this.__socket.readyState === 1 && this.p) {
            var type_id = typeof type === 'string' ? this.p[type] : type;
            if (type_id >= 0) {
                var packet = {
                    'TYP': type_id,
                    'SID': this.__options.sid,
                    'TME': Math.round((new Date).getTime() / 1000)
                };
                if (typeof payload !== 'undefined') packet.PLD = payload;
                this.__socket.send(this.__encode(packet));
            } else console.warn('Unknown Warlock packet type: ' + type);
        } else if (queue !== false)
            this.__messageQueue.push([type, payload]);
    };
    this.__log = function (msg) {
        console.log('Warlock: ' + msg);
    };
    this.connected = function () {
        return this.__socket && this.__socket.readyState === 1;
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
        this.__send('sync', { 'access_key': this.admin_key }, true);
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
        this.__send('trigger', {
            'id': event_id,
            'data': data,
            'echo': echo_self === true
        }, true);
        return this;
    };
    /* Admin Commands (These require sync with admin key) */
    this.stop = function (delay_sec) {
        this.__send('shutdown', delay_sec > 0 ? { delay: delay_sec } : null);
        return this;
    };
    this.enable = function (service) {
        this.__send('enable', service);
        return this;
    };
    this.disable = function (service) {
        this.__send('disable', service);
        return this;
    };
    this.service = function (name) {
        this.__send('service', name);
        return this;
    };
    this.enableEncoding = function () {
        this.__options.encoded = true;
        return this;
    };
    this.status = function () {
        this.__send('status');
        return this;
    };
    this.spawn = function (service, params) {
        this.__send('spawn', { 'name': service, 'params': params });
        return this;
    };
    this.kill = function (service) {
        this.__send('kill', { name: service });
        return this;
    };
    this.signal = function (service, event_id, data) {
        this.__send('signal', {
            'service': service,
            'id': event_id,
            'data': data
        }, true);
        return this;
    };
    this.__kv_send = function (msg, payload, callback) {
        if (!(msg in this.__callbacks)) this.__callbacks[msg] = [];
        this.__callbacks[msg].push(callback);
        this.__send(msg, payload);
    };
    this.get = function (key, c) {
        this.__kv_send('kvget', { k: key }, c);
    };
    this.set = function (key, value, c) {
        this.__kv_send('kvset', { k: key, v: value }, c);
    };
    this.has = function (key, c) {
        this.__kv_send('kvhas', { k: key }, c);
    };
    this.del = function (key, c) {
        this.__kv_send('kvdel', { k: key }, c);
    };
    this.list = function (key, c) {
        this.__kv_send('kvlist', { k: key }, c);
    };
    this.clear = function (c) {
        this.__kv_send('kvclear', {}, c);
    };
    this.pull = function (key, c) {
        this.__kv_send('kvpull', { k: key }, c);
    };
    this.push = function (key, value, c) {
        this.__kv_send('kvpush', { k: key, v: value }, c);
    };
    this.pop = function (key, c) {
        this.__kv_send('kvpop', { k: key }, c);
    };
    this.shift = function (key, c) {
        this.__kv_send('kvshift', { k: key }, c);
    };
    this.unshift = function (key, value, c) {
        this.__kv_send('kvclear', { k: key, v: value }, c);
    };
    this.incr = function (key, c) {
        this.__kv_send('kvincr', { k: key }, c);
    };
    this.decr = function (key, c) {
        this.__kv_send('kvclear', { k: key }, c);
    };
    this.keys = function (key, c) {
        this.__kv_send('kvclear', { k: key }, c);
    };
    this.values = function (key, c) {
        this.__kv_send('kvclear', { k: key }, c);
    };
    this.guid = this.__getGUID();
    this.__log('GUID=' + this.guid);
    this.__log('Server ID=' + this.__options.sid);
    if (this.__options.connect === true) this.connect();
    return this;
};
