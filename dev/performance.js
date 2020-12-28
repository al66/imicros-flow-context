"use strict";
const { ServiceBroker } = require("moleculer");
const { Context } = require("../index");
const { v4: uuid } = require("uuid");

process.env.CASSANDRA_CONTACTPOINTS = "192.168.2.124";
process.env.CASSANDRA_DATACENTER = "datacenter1";
process.env.CASSANDRA_KEYSPACE = "imicros_flow";

// mock keys service
const keyId = uuid();
const KeysMock = {
    name: "keys",
    actions: {
        getOek: {
            handler(ctx) {
                if (!ctx.params || !ctx.params.service) throw new Error("Missing service name");
                return {
                    id: keyId,
                    key: "mySecret"
                };
            }
        }
    }
};

const packages = 10;
const batch = 2000;

const start = async function () {

    let broker = new ServiceBroker({
        logger: console,
        logLevel: "info" //"debug"
    });
    await broker.createService(KeysMock);
    await broker.createService(Context, Object.assign({ 
        settings: { 
            cassandra: {
                contactPoints: process.env.CASSANDRA_CONTACTPOINTS || "127.0.0.1", 
                datacenter: process.env.CASSANDRA_DATACENTER || "datacenter1", 
                keyspace: process.env.CASSANDRA_KEYSPACE || "imicros_flow" 
            },
            services: {
                keys: "keys"
            }
        },
        dependencies: ["keys"]
    }));
    await broker.start();
    return broker;
};

const stop = async function (broker) {
    await broker.stop();
};

const write = async function (broker) {
    
    let instanceId = uuid();
    let opts = { meta: { user: { id: uuid() , email: "test@host.com" }, ownerId: uuid() } };
    
    
    function writeSingle(i) {
        let params = {
            instanceId: instanceId,
            key: "a-" + i,
            value: { msg: "say hello to the world" }
        };
        return broker.call("context.add", params, opts);
    }
    
    // packages
    for (let n=0; n < packages; n++) {
        let writes = [];
        // max due to connections
        for (let i=0; i < batch; i++ ) {
            writes.push(writeSingle(i));
        }
        await Promise.all(writes);
    }
    
};

const exec = async function () {
    let broker = await start();
    
    let startTime = Date.now();
    await write(broker);
    console.log("Run time:", Date.now() - startTime);
    console.log("Written records:", packages * batch );
    
    await stop(broker);
};

exec();



