'use strict';
const mqtt = require("mqtt");
const fs = require("fs");

//--Backend class to maintain the MQTT clients
module.exports = class DynMQTT {
    static clients = {};
    static client_stats = {};

    static findTopic(received, subscribtions) {
        for (let subscribed of subscribtions) {
            let regex = subscribed.replace(/\+/g, '[^\/]+')
            regex = regex.replace(/\#/g, '.*')
            regex = regex.replace(/\$/g, '\\$')
            regex = regex.replace(/\//g, '\\/')
            regex = '^' + regex + '$'

            if (received.match(regex) != null) {
                return subscribed
            }
        }
        return false
    }

    static closeClients() {
        for (const [id, client] of Object.entries(DynMQTT.clients)) {
            client.close();
        }
        DynMQTT.clients = {};
    }

    static closeClient(id) {
        let client = this.getConnectedClient(id)
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
        let client = this.getConnectedClient(client_id)
        if (client) {
            console.info(`MQTT: Client already connected: ${client_id}`);
        } else {
            console.info(`MQTT: Creating new client: ${client_id}`);
            DynMQTT.clients[client_id] = new DynMQTT(client_id, config.payload, status_callback);
        }
        return DynMQTT.clients[client_id]
    }

    static getConnectedClient(client_id) {
        let client = this.getClient(client_id);
        if (client && client.status == "connected") return client;
        else false;
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

        this.reconnect_t = config.reconnect || 0;
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

        this.connection.on('close', () => {
            for (let topic of Object.keys(this.subscriptions))
                this.unsubscribe(topic);
            this.change_status("closed");
        });

        this.connection.on('message', (topic, message) => {
            let subTopic = DynMQTT.findTopic(topic, Object.keys(this.subscriptions))
            if (subTopic) {
                this.subscriptions[subTopic](topic, message);
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

    async subscribe(topic, callback) {
        let subTopic = DynMQTT.findTopic(topic, Object.keys(this.subscriptions))
        if (subTopic) {
            console.info(`MQTT: ${subTopic} already subscribed`);
        } else {
            try {
                //Default callback
                this.subscriptions[topic] = callback;
                let grantedList = await this.connection.subscribeAsync(topic);
                console.info(`MQTT: Subscribed to: ${grantedList[0].topic}`);
                this.change_status(this.status);
            } catch (err) {
                console.error(`MQTT: ${err}`);
                delete this.subscriptions[topic];
            }
        }
    }

    async unsubscribe(topic) {
        let subTopic = DynMQTT.findTopic(topic, Object.keys(this.subscriptions))
        if (subTopic) {
            try {
                await this.connection.unsubscribeAsync(topic);
                delete this.subscriptions[topic];
                this.change_status(this.status);
                console.info(`MQTT: Unsubscribed from: ${topic}`);
                return true;
            } catch (err) {
                console.error(`MQTT: ${err}`);
                return false;
            }
        } else console.warn(`MQTT: ${topic} not unsubscribed`);
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