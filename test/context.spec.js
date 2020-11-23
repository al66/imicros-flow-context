"use strict";
const { ServiceBroker } = require("moleculer");
const { Context } = require("../index");
const { v4: uuid } = require("uuid");

const timestamp = Date.now();
const ownerId = `owner-${timestamp}`;
const instanceId= uuid();
const keys = {
    current: uuid(),
    previous: uuid()
};

const AclMock = {
    localAction(next, action) {
        return async function(ctx) {
            ctx.meta = Object.assign(ctx.meta,{
                ownerId: ownerId,
                acl: {
                    accessToken: "this is the access token",
                    ownerId: ownerId,
                    unrestricted: true
                },
                user: {
                    id: `1-${timestamp}` , 
                    email: `1-${timestamp}@host.com` 
                }
            });
            ctx.broker.logger.debug("ACL meta data has been set", { meta: ctx.meta, action: action });
            return next(ctx);
        };
    }    
};

// mock keys service
const KeysMock = {
    name: "keys",
    actions: {
        getOek: {
            handler(ctx) {
                if (!ctx.params || !ctx.params.service) throw new Error("Missing service name");
                if ( ctx.params.id == keys.previous ) {
                    return {
                        id: keys.previous,
                        key: "myPreviousSecret"
                    };    
                }
                return {
                    id: keys.current,
                    key: "mySecret"
                };
            }
        }
    }
};

describe("Test context service", () => {

    let broker, service, opts, keyService;
    beforeAll(() => {
    });
    
    afterAll(async () => {
    });
    
    describe("Test create service", () => {

        it("it should start the broker", async () => {
            broker = new ServiceBroker({
                middlewares:  [AclMock],
                logger: console,
                logLevel: "info" //"debug"
            });
            keyService = await broker.createService(KeysMock);
            service = await broker.createService(Context, Object.assign({ 
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
            expect(service).toBeDefined();
            expect(keyService).toBeDefined();
        });

    });
    
    describe("Test context ", () => {

        beforeEach(() => {
            opts = { meta: { user: { id: `1-${timestamp}` , email: `1-${timestamp}@host.com` }, ownerId: `g-${timestamp}` } };
        });

        it("it should create context A with key a1 ", () => {
            opts = { };
            let params = {
                instanceId: instanceId,
                key: "a1",
                value: { msg: "say hello to the world" }
            };
            return broker.call("context.add", params, opts).then(res => {
                expect(res).toBeDefined();
                expect(res).toEqual(true);
            });
        });
        
        it("it should add key a2 to context A", () => {
            opts = { };
            let params = {
                instanceId: instanceId,
                key: "a2",
                value: { x: 5, y: 7.6 }
            };
            return broker.call("context.add", params, opts).then(res => {
                expect(res).toBeDefined();
                expect(res).toEqual(true);
            });
        });
        
        it("it should add key a3 to context A", () => {
            opts = { };
            let params = {
                instanceId: instanceId,
                key: "a3",
                value: { val: "something else" }
            };
            return broker.call("context.add", params, opts).then(res => {
                expect(res).toBeDefined();
                expect(res).toEqual(true);
            });
        });

        it("it should get key a2 of context A", () => {
            opts = { };
            let a2 = { x: 5, y: 7.6 };
            let params = {
                instanceId: instanceId,
                key: "a2"
            };
            return broker.call("context.get", params, opts).then(res => {
                expect(res).toBeDefined();
                expect(res).toEqual(a2);
            });
        });
        
        it("it should get key a1 of context A", () => {
            opts = { };
            let a1 = { msg: "say hello to the world" };
            let params = {
                instanceId: instanceId,
                key: "a1"
            };
            return broker.call("context.get", params, opts).then(res => {
                expect(res).toBeDefined();
                expect(res).toEqual(a1);
            });
        });
        
        it("it should get key a1 and a3 of context A", () => {
            opts = { };
            let a1 = { msg: "say hello to the world" };
            let a3 = { val: "something else" };
            let params = {
                instanceId: instanceId,
                keys: ["a1", "a3"]
            };
            return broker.call("context.getKeys", params, opts).then(res => {
                expect(res).toBeDefined();
                expect(res.a1).toEqual(a1);
                expect(res.a3).toEqual(a3);
            });
        });
        
        it("it should get key a3 of context A", () => {
            opts = { };
            let a3 = { val: "something else" };
            let params = {
                instanceId: instanceId,
                keys: ["a3"]
            };
            return broker.call("context.getKeys", params, opts).then(res => {
                expect(res).toBeDefined();
                expect(res.a1).not.toBeDefined();
                expect(res.a3).toEqual(a3);
            });
        });
        
        it("it should get list of keys", () => {
            opts = { };
            let params = {
                instanceId: instanceId,
                keys: []
            };
            return broker.call("context.getKeys", params, opts).then(res => {
                expect(res).toBeDefined();
                expect(res.length).toEqual(3);
                expect(res[0]).toEqual({ instanceId: instanceId, ownerId: ownerId, key:"a1" });
                expect(res).toContainEqual({ instanceId: instanceId, ownerId: ownerId, key:"a1" });
                expect(res).toContainEqual({ instanceId: instanceId, ownerId: ownerId, key:"a2" });
                expect(res).toContainEqual({ instanceId: instanceId, ownerId: ownerId, key:"a3" });
            });
        });
        
        it("it should remove key a3 from context A", () => {
            opts = { };
            let params = {
                instanceId: instanceId,
                key: "a3"
            };
            return broker.call("context.remove", params, opts).then(res => {
                expect(res).toBeDefined();
                expect(res).toEqual(true);
            });
        });
        
        /*
        it("it should get context A w/o key a3", () => {
            opts = { };
            let a1 = { msg: "say hello to the world" };
            let a2 = { x: 5, y: 7.6 };
            let params = {
                instance: "A-" + timestamp
            };
            return broker.call("context.get", params, opts).then(res => {
                expect(res.a1).toBeDefined();
                expect(res.a2).toBeDefined();
                expect(res.a3).not.toBeDefined();
                expect(res.a1).toEqual(a1);
                expect(res.a2).toEqual(a2);
            });
        });
        
        it("it should return an empty object for a non-existing context", () => {
            opts = { };
            let params = {
                instance: "B-" + timestamp
            };
            return broker.call("context.get", params, opts).then(res => {
                expect(res).toBeDefined();
                expect(res).toEqual({});
            });
        });
        
        it("it should return an empty object for an empty instance", () => {
            opts = { };
            let params = {
                instance: ""
            };
            return broker.call("context.get", params, opts).then(res => {
                expect(res).toBeDefined();
                expect(res).toEqual({});
            });
        });
        */
        
    });

    describe("Test token ", () => {

        let opts, processId = uuid(), instanceId = uuid(), elementId = uuid();
        
        let token = ["ACTIVITY.ACTIVATED", "ACTIVITY.PREPARED", "ACTIVITY.COMPLETED"].map(status => {
            return {
                processId: processId,
                instanceId: instanceId,
                elementId: elementId,
                status: status
            };
        });
        
        beforeEach(() => {
            opts = { meta: { user: { id: `1-${timestamp}` , email: `1-${timestamp}@host.com` }, ownerId: `g-${timestamp}` } };
        });

        
        it("it should save token A", () => {
            let params = {
                processId: token[0].processId,
                instanceId: token[0].instanceId,
                elementId: token[0].elementId,
                token: token[0]
            };
            return broker.call("context.saveToken", params, opts).then(res => {
                expect(res).toEqual(true);
            });
            
        });
 
        it("it should return token A", () => {
            let params = {
                processId: token[0].processId,
                instanceId: token[0].instanceId,
                elementId: token[0].elementId
            };
            return broker.call("context.getToken", params, opts).then(res => {
                expect(res).toBeDefined();
                expect(res.length).toEqual(1);
                expect(res).toContainEqual(token[0]);
            });
            
        });
 
        
        it("it should save token B", () => {
            let params = {
                processId: token[1].processId,
                instanceId: token[1].instanceId,
                elementId: token[1].elementId,
                token: token[1]
            };
            return broker.call("context.saveToken", params, opts).then(res => {
                expect(res).toEqual(true);
            });
            
        });
 
        it("it should return token A and B", () => {
            let params = {
                processId: token[0].processId,
                instanceId: token[0].instanceId,
                elementId: token[0].elementId
            };
            return broker.call("context.getToken", params, opts).then(res => {
                expect(res).toBeDefined();
                expect(res.length).toEqual(2);
                expect(res).toContainEqual(token[0]);
                expect(res).toContainEqual(token[1]);
            });
            
        });

        it("it should remove token A", () => {
            let params = {
                processId: token[0].processId,
                instanceId: token[0].instanceId,
                elementId: token[0].elementId,
                token: token[0]
            };
            return broker.call("context.removeToken", params, opts).then(res => {
                expect(res).toEqual(true);
            });
            
        });

        it("it should return token B", () => {
            let params = {
                processId: token[1].processId,
                instanceId: token[1].instanceId,
                elementId: token[1].elementId
            };
            return broker.call("context.getToken", params, opts).then(res => {
                expect(res).toBeDefined();
                expect(res.length).toEqual(1);
                expect(res).toContainEqual(token[1]);
            });
            
        });
 
        it("it should remove token B", () => {
            let params = {
                processId: token[1].processId,
                instanceId: token[1].instanceId,
                elementId: token[1].elementId,
                token: token[1]
            };
            return broker.call("context.removeToken", params, opts).then(res => {
                expect(res).toEqual(true);
            });
            
        });

        it("it should return an empty array", () => {
            let params = {
                processId: token[0].processId,
                instanceId: token[0].instanceId,
                elementId: token[0].elementId
            };
            return broker.call("context.getToken", params, opts).then(res => {
                expect(res).toBeDefined();
                expect(res.length).toEqual(0);
            });
            
        });

    });
    
    describe("Test stop broker", () => {
        it("should stop the broker", async () => {
            expect.assertions(1);
            await broker.stop();
            expect(broker).toBeDefined();
        });
    });
    
});