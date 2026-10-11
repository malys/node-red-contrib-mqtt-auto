'use strict';
const mqtt = require("mqtt");
const fs = require("fs");

//--Backend class to maintain the MQTT clients
module.exports = class DynMQTT {
    static clients = {};
    static client_stats = {};

    // --- MQTT filter matching, level by level (+ = one level, # = rest)
    static topicMatches(filter, topic) {
        const f = filter.split('/');
        const t = topic.split('/');
        // $SYS-like topics are never matched by a leading wildcard
        if (t[0].startsWith('$') && (f[0] === '+' || f[0] === '#')) return false;
        for (let i = 0; i < f.length; i++) {
            if (f[i] === '#') return true;
            if (i >= t.length) return false;
            if (f[i] !== '+' && f[i] !== t[i]) return false;
        }
        return f.length === t.length;
    }

    // --- All subscribed filters matching a received topic
    static findTopics(received, subscriptions) {
        return subscriptions.filter(s => DynMQTT.topicMatches(s, received));
    }

    static closeClients() {
        for (const [id, client] of Object.entries(DynMQTT.clients)) {
            client.close();
        }
        DynMQTT.clients = {};
    }

    static closeClient(id) {
        let client = this.getClient(id)
        if (client) {
            client.close();
            delete DynMQTT.clients[id];
            return true;
        } else {
            return false;
        }
    }

    static createClient(config, status_callback) {
        const client_id = config.payload.client_id;
        const existing = this.getClient(client_id);
        // A connected client with the same settings is reused. Otherwise (new
        // credentials/host, or still reconnecting) it is replaced and its
        // subscriptions are carried over.
        if (existing && existing.status === "connected" && existing.settings === JSON.stringify(config.payload)) {
            console.info(`MQTT: Client already connected: ${client_id}`);
            existing.status_callback = status_callback;
            return existing;
        }
        console.info(`MQTT: Creating new client: ${client_id}`);
        const client = new DynMQTT(client_id, config.payload, status_callback);
        if (existing) {
            client.subscriptions = existing.subscriptions;
            existing.subscriptions = {};
            existing.connection.end(true);
            // Queued by mqtt.js until 'connect'
            for (const topic of Object.keys(client.subscriptions))
                client.connection.subscribe(topic, err => err && console.error(`MQTT: resubscribe ${topic}: ${err}`));
        }
        DynMQTT.clients[client_id] = client;
        return client;
    }

    static listClients() {
        let ret = {};

        for (const [id, client] of Object.entries(DynMQTT.clients)) {
            ret[id] = { "status": client.status, "subscriptions": [] }

            for (let topic of Object.keys(client.subscriptions))
                ret[id]["subscriptions"].push(topic);
        }
        return ret;
    }

    static getClient(client_id) {
        if (DynMQTT.clients.hasOwnProperty(client_id)) {
            return DynMQTT.clients[client_id]
        } else {
            //console.error("Client " + client_id + " is not known");
            return false;
        }
    }

    constructor(client_id, config, status_callback) {
        this.client_id = client_id;
        this.host = config.host;
        this.user = config.user;
        this.password = config.password;

        this.key_path = config.key || false;
        this.cert_path = config.cert || false;
        this.ca_path = config.ca || false;

        this.settings = JSON.stringify(config);
        // README documents reconnect_t (0 = off, 1 = on); values > 1 are a period in ms
        let reconnect = config.reconnect_t ?? config.reconnect ?? 0;
        reconnect = (reconnect === true || reconnect === 'true') ? 1 : Number(reconnect) || 0;
        this.reconnect_t = reconnect > 1 ? reconnect : (reconnect === 1 ? 1000 : 0);
        this.rap = config.rap || true; // Retain as Published MQTT 5.0 flag
        this.rh = config.rh || true; //Retain Handling MQTT 5.0 

        this.subscriptions = {};
        this.status = false;
        this.status_callback = status_callback;

        this.change_status('initiated');

        let protocole = 'mqtt://';
        let options = {
            clientId: this.client_id,
            username: this.user,
            password: this.password,
            reconnectPeriod: this.reconnect_t
        }

        if (this.cert_path) {
            options = Object.assign(options, {
                rejectUnauthorized: false,
                key: fs.readFileSync(this.key_path),
                cert: fs.readFileSync(this.cert_path),
                ca: [fs.readFileSync(this.ca_path)],
            });
            protocole = 'mqtts://';
        }

        this.connection = mqtt.connect(protocole + this.host, options);

        /*Client events*/
        this.connection.on('connect', () => {
            this.change_status("connected");
            //this.status_callback({ summary: { fill: "green", shape: "dot", text: "Connected" } });
        });

        this.connection.on('error', (err) => {
            this.change_status("error");
            console.error(`MQTT:  connection: ${err}`);
            this.status_callback({ summary: { fill: "red", shape: "dot", text: err } });
        });

        this.connection.on('reconnect', () => {
            this.change_status("reconnecting");
            this.status_callback({ summary: { fill: "red", shape: "dot", text: "Reconnection" } });
        });

        this.connection.on('disconnect', () => {
            this.change_status("disconnect");
            this.status_callback({ summary: { fill: "red", shape: "dot", text: "Disconnected" } });
        });

        this.connection.on('offline', () => {
            this.change_status("offline");
            this.status_callback({ summary: { fill: "red", shape: "dot", text: "Offline" } });

        });

        // 'close' also fires before each reconnection: subscriptions are kept
        // (mqtt.js re-sends them on reconnect). They are only cleared by close().
        this.connection.on('close', () => {
            this.change_status("closed");
        });

        this.connection.on('message', (topic, message) => {
            const filters = DynMQTT.findTopics(topic, Object.keys(this.subscriptions));
            if (filters.length) {
                // An owner subscribed to overlapping filters gets the message once.
                // ponytail: a broker may still send one copy per matching filter
                // (mosquitto does); MQTT 5 subscription identifiers would dedupe them.
                const callbacks = new Map();
                for (const f of filters)
                    for (const [owner, cb] of this.subscriptions[f]) callbacks.set(owner, cb);
                for (const cb of callbacks.values()) {
                    try { cb(topic, message); } catch (err) { console.error(`MQTT: callback ${topic}: ${err}`); }
                }
            } else {
                console.error(`MQTT: Rec msg w/o subscription on ${topic}`);
            }
        });

    }

    change_status(status) {
        console.debug(`MQTT: ` + this.client_id + ": " + this.status + " -> " + status);
        let ret = {
            "mqtt_change": {
                "client_id": this.client_id,
                "status_last": this.status,
                "status_new": status
            }
        }

        // --- Take care of the previous status (only if it is set)
        if (this.status) {
            if (DynMQTT.client_stats.hasOwnProperty(this.status))
                DynMQTT.client_stats[this.status]--;
            else
                DynMQTT.client_stats[this.status] = 0;
        }

        // --- Take care of the new status
        this.status = status;
        // --- Populate the response with a list of the current clients
        ret["payload"] = DynMQTT.listClients()

        if (this.status != 'closed') { // -- we don't count closed (they will never decrement if reconnect is disabled)
            if (DynMQTT.client_stats.hasOwnProperty(this.status))
                DynMQTT.client_stats[this.status]++;
            else
                DynMQTT.client_stats[this.status] = 1;
        }

        let txt = "";
        for (const [name, count] of Object.entries(DynMQTT.client_stats)) {
            let n = (name.charAt(0).toUpperCase() + name.slice(1)).trim()
            if (parseInt(count) > 0)
                txt = txt + n + ": " + parseInt(count);
            else
                delete DynMQTT.client_stats[name]; // -- auto-sanitize 
        }
        ret.summary = { fill: "green", shape: "dot", text: txt };
        this.status_callback(ret);
    }

    // subscriptions = { filter: Map(owner -> callback) }
    // owner identifies the subscriber (Node-RED node id): a node re-subscribing
    // replaces its own callback instead of being ignored.
    // Resolves true once the broker granted the filter, false on failure.
    subscribe(topic, callback, owner = 'default') {
        let owners = this.subscriptions[topic];
        if (!owners) {
            owners = this.subscriptions[topic] = new Map();
            // Shared by every owner subscribing while the SUBACK is pending
            owners.ready = this.connection.subscribeAsync(topic).then(grantedList => {
                console.info(`MQTT: Subscribed to: ${grantedList[0] ? grantedList[0].topic : topic}`);
                this.change_status(this.status);
                return true;
            }, err => {
                console.error(`MQTT: ${err}`);
                if (this.subscriptions[topic] === owners) delete this.subscriptions[topic];
                return false;
            });
        }
        owners.set(owner, callback);
        return owners.ready;
    }

    // Without owner: removes the filter for everyone.
    // With owner: removes only that owner; the broker is unsubscribed when no owner is left.
    async unsubscribe(topic, owner) {
        const owners = this.subscriptions[topic];
        if (!owners) {
            console.warn(`MQTT: ${topic} not unsubscribed (unknown)`);
            return false;
        }
        if (owner !== undefined) {
            owners.delete(owner);
            if (owners.size) return true;
        }
        delete this.subscriptions[topic];
        try {
            await this.connection.unsubscribeAsync(topic);
            this.change_status(this.status);
            console.info(`MQTT: Unsubscribed from: ${topic}`);
            return true;
        } catch (err) {
            console.error(`MQTT: ${err}`);
            return false;
        }
    }

    // Removes every callback of an owner (called when a node is deleted).
    // Callbacks are dropped synchronously; the returned promise only tracks the broker.
    unsubscribeOwner(owner) {
        return Promise.all(Object.entries(this.subscriptions)
            .filter(([, owners]) => owners.has(owner))
            .map(([topic]) => this.unsubscribe(topic, owner)));
    }

    async publish(topic, payload, options) {
        if (this.status === "connected") {
            let send_payload = String(payload);
            
            await this.connection.publishAsync(topic, send_payload, options);
            return true;
        }
        return false;
    }

    close() {
        this.connection.end();
        this.subscriptions = {};
        delete DynMQTT.clients[this.client_id];
    }

}