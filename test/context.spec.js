"use strict";
const { ServiceBroker } = require("moleculer");
const { Context } = require("../index");

const timestamp = Date.now();

describe("Test context service", () => {

    let broker, service, opts;
    beforeAll(() => {
    });
    
    afterAll(async () => {
    });
    
    describe("Test Redis not available", () => {

        it("it should stop broker", async () => {
            broker = new ServiceBroker({
                logger: console,
                logLevel: "info" //"debug"
            });
            service = await broker.createService(Context, Object.assign({ 
                settings: { 
                    redis: {
                        port: 9999,
                        host: "127.0.0.1",
                        password: "",
                        db: 0,
                    }
                }
            }));
            await expect(broker.start()).rejects.toThrow("connect ECONNREFUSED 127.0.0.1:9999");
        });

    });
    
    describe("Test create service", () => {

        it("it should start the broker", async () => {
            broker = new ServiceBroker({
                logger: console,
                logLevel: "info" //"debug"
            });
            service = await broker.createService(Context, Object.assign({ 
                settings: { 
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
                contextId: "A-" + timestamp,
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
                contextId: "A-" + timestamp,
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
                contextId: "A-" + timestamp,
                key: "a3",
                value: { val: "something else" }
            };
            return broker.call("context.add", params, opts).then(res => {
                expect(res).toBeDefined();
                expect(res).toEqual(true);
            });
        });
        
        it("it should get context A", () => {
            opts = { };
            let a1 = { msg: "say hello to the world" };
            let a2 = { x: 5, y: 7.6 };
            let a3 = { val: "something else" };
            let params = {
                contextId: "A-" + timestamp
            };
            return broker.call("context.get", params, opts).then(res => {
                expect(res.a1).toBeDefined();
                expect(res.a2).toBeDefined();
                expect(res.a3).toBeDefined();
                expect(res.a1).toEqual(a1);
                expect(res.a2).toEqual(a2);
                expect(res.a3).toEqual(a3);
            });
        });
        
        it("it should remove key a3 from context A", () => {
            opts = { };
            let params = {
                contextId: "A-" + timestamp,
                key: "a3"
            };
            return broker.call("context.rollback", params, opts).then(res => {
                expect(res).toBeDefined();
                expect(res).toEqual(true);
            });
        });
        
        it("it should get context A w/o key a3", () => {
            opts = { };
            let a1 = { msg: "say hello to the world" };
            let a2 = { x: 5, y: 7.6 };
            let params = {
                contextId: "A-" + timestamp
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
                contextId: "B-" + timestamp
            };
            return broker.call("context.get", params, opts).then(res => {
                expect(res).toBeDefined();
                expect(res).toEqual({});
            });
        });
        
        it("it should return an empty object for an empty contextId", () => {
            opts = { };
            let params = {
                contextId: ""
            };
            return broker.call("context.get", params, opts).then(res => {
                expect(res).toBeDefined();
                expect(res).toEqual({});
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