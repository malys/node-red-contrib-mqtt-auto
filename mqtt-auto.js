'use strict';

const DynMQTT = require("./DynMQTT.js");
const { destr } = require("destr");
const main = function (RED) {

    // =========== Connection manager ===========
    RED.nodes.registerType("mqtt-auto-connect", function (config) {
        RED.nodes.createNode(this, config);
        var node = this;
        node.on('close', function (done) {
            DynMQTT.closeClients();
            done();
        });
        node.on('input', function (msg, send, done) {
            DynMQTT.createClient(msg, function (new_status) {
                node.status(new_status.summary);
                node.send(new_status)
            })
            done();
        });
    });

    RED.nodes.registerType("mqtt-auto-disconnect", function (config) {
        RED.nodes.createNode(this, config);
        var node = this;
        node.on('input', function (msg, send, done) {
            let result = DynMQTT.closeClient(msg.client_id)
            if (result) {
                done();
            } else {
                done("Client not found!");
            }
        });
    });


    // =========== Publisher ===========
    RED.nodes.registerType("mqtt-auto-publish", function (config) {
        RED.nodes.createNode(this, config);
        var node = this;
        node.on('input', async function (msg, send, done) {
            let client = DynMQTT.getClient(msg.client_id);
            let options = msg.options || {};
            if (client && await client.publish(msg.topic, msg.payload, options)) {
                done();
            }
            else {
                node.warn("Client not known or not connected - sending msg back for re-looping");
                send(msg);
                done();
            }
        })
    });


    // =========== Subscriber ===========
    RED.nodes.registerType("mqtt-auto-subscribe", function (config) {
        RED.nodes.createNode(this, config);
        var node = this;
        node.on('input', async function (msg, send, done) {
            let client = DynMQTT.getClient(msg.client_id);
            if (client) {
                let topics = [msg.topic].flat()
                const nodeId = node.id;
                const failed = [];
                for (let t of topics) {
                    // -- actualTopic contains the real topic the message was received at (no wildards)
                    // -- node.id as owner: re-subscribing replaces this node's callback
                    // -- node looked up at delivery: a redeployed node (same id) keeps receiving
                    let ok = await client.subscribe(t, function (actualTopic, message) {
                        const target = RED.nodes.getNode(nodeId);
                        if (target) target.send({
                            "payload": destr(message.toString()),
                            "client_id": client.client_id,
                            "topic": actualTopic
                        })
                    }, nodeId)
                    if (!ok) failed.push(t);
                }
                done(failed.length ? "Subscription failed: " + failed.join(", ") : undefined);
            } else
                done("Client " + msg.client_id + " for Subscription not known!");
        });

        // Deleted node: drop its callbacks. Redeployed/restarted node: keep them,
        // the new instance (same id) receives through RED.nodes.getNode.
        // Broker unsubscribe is not awaited: it never settles while offline.
        node.on('close', function (removed, done) {
            if (removed) {
                for (const client of Object.values(DynMQTT.clients))
                    client.unsubscribeOwner(node.id).catch(() => { });
            }
            done();
        });
    });

    RED.nodes.registerType("mqtt-auto-unsubscribe", function (config) {
        RED.nodes.createNode(this, config);
        var node = this;
        node.on('input', async function (msg, send, done) {
            let client = DynMQTT.getClient(msg.client_id);
            if (client && await client.unsubscribe(msg.topic)) {
                done();
            } else
                done("Client " + msg.client_id + " for Un-Subscription not known!");
        });
    });

    // =========== Status ===========
    RED.nodes.registerType("mqtt-auto-status", function (config) {
        RED.nodes.createNode(this, config);
        var node = this;
        node.on('input', function (msg, send, done) {
            send({ "payload": DynMQTT.listClients() })
            done();
        });
    });
}

module.exports = main;