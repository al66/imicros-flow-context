"use strict";
const { ServiceBroker } = require("moleculer");
const { Context } = require("../index");
const uuidv4 = require("uuid/v4");

const timestamp = Date.now();
const ownerId = `owner-${timestamp}`;
const instance = uuidv4();

const AclMock = {
    localAction(next, action) {
        return async function(ctx) {
            ctx.meta = Object.assign(ctx.meta,{
                acl: {
                    accessToken: "this is the access token",
                    ownerId: ownerId,
                    unrestricted: true
                },
                user: {
                    idToken: "this is the id token",
                    id: `1-${timestamp}` , 
                    email: `1-${timestamp}@host.com` 
                }
            });
            ctx.broker.logger.debug("ACL meta data has been set", { meta: ctx.meta, action: action });
            return next(ctx);
        };
    }    
};

describe("Test context service", () => {

    let broker, service, opts;
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
            service = await broker.createService(Context, Object.assign({ 
                settings: { 
                    cassandra: {
                        contactPoints: process.env.CASSANDRA_CONTACTPOINTS || "127.0.0.1" 
                    },
                    redis: {
                        port: process.env.REDIS_PORT || 6379,
                        host: process.env.REDIS_HOST || "127.0.0.1",
                        password: process.env.REDIS_AUTH || "",
                        db: process.env.REDIS_DB || 0,
                    }
                }
            }));
            await broker.start();
            expect(service).toBeDefined();
        });

    });
    
    describe("Test context ", () => {

        beforeEach(() => {
            opts = { meta: { user: { id: `1-${timestamp}` , email: `1-${timestamp}@host.com` }, groupId: `g-${timestamp}` } };
        });

        it("it should create context A with key a1 ", () => {
            opts = { };
            let params = {
                instance: instance,
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
                instance: instance,
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
                instance: instance,
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
                instance: instance,
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
                instance: instance,
                key: "a1"
            };
            return broker.call("context.get", params, opts).then(res => {
                expect(res).toBeDefined();
                expect(res).toEqual(a1);
            });
        });
        
        it("it should remove key a3 from context A", () => {
            opts = { };
            let params = {
                instance: instance,
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

    describe("Test stop broker", () => {
        it("should stop the broker", async () => {
            expect.assertions(1);
            await broker.stop();
            expect(broker).toBeDefined();
        });
    });
    
});