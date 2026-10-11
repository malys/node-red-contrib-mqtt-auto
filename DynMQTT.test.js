'use strict';
// Run: npm test
const assert = require("assert");
const DynMQTT = require("./DynMQTT.js");

// --- Topic matching
const m = DynMQTT.topicMatches;
assert(m("a/+", "a/b"));
assert(!m("a/+", "a/b/c"));
assert(m("a/+", "a/"));
assert(m("a/#", "a/b/c"));
assert(m("a/#", "a"));
assert(!m("a/b", "a"));
assert(!m("a.b", "axb"));
assert(!m("#", "$SYS/x"));
assert(!m("+/x", "$SYS/x"));
assert(m("$SYS/#", "$SYS/x"));
assert.deepStrictEqual(DynMQTT.findTopics("a/b", ["a/+", "a/b", "c"]), ["a/+", "a/b"]);

// --- Owners subscribing while the SUBACK is pending share its result
(async () => {
    const client = Object.create(DynMQTT.prototype);
    let reject;
    client.subscriptions = {};
    client.connection = { subscribeAsync: () => new Promise((_, r) => reject = r) };
    const a = client.subscribe("x", () => { }, "A");
    const b = client.subscribe("x", () => { }, "B");
    const origError = console.error;
    console.error = () => { };
    reject(new Error("denied"));
    assert.deepStrictEqual(await Promise.all([a, b]), [false, false]);
    console.error = origError;
    assert(!client.subscriptions.x);
    console.log("ok");
})();
