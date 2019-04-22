/**
 * @license MIT, imicros.de (c) 2019 Andreas Leinen
 */
"use strict";

const Redis = require("ioredis");
const { Serializer } = require("./serializer/base");

module.exports = {
    name: "context",
    
    /**
     * Service settings
     */
    settings: {},

    /**
     * Service metadata
     */
    metadata: {},

    /**
     * Service dependencies
     */
    //dependencies: [],	

    /**
     * Actions
     */
    actions: {
        
        /**
         * Add payload to context 
         * 
         * @param {String} contextId - uuid
         * @param {String} key  -   PARENT = ContextId of calling process   or
         *                          START = inital event   or
         *                          process step in format <process>.<step>.<cycle>
         * @param {String} value -  Key PARENT: ContextId
         *                          Key START: payload of initial event
         *                          Key processs step: result of the executed process stap  
         * 
         * @returns {Boolean} result
         */
        add: {
            params: {
                contextId: { type: "string" },
                key: { type: "string" },
                value: { type: "any" }
            },
            async handler(ctx) {
                let value = await this.serializer.serialize(ctx.params.value);
                //HSET
                try {
                    await this.client.hset(ctx.params.contextId, ctx.params.key, value);
                    return true;
                } catch (err) /* istanbul ignore next */ {
                    this.logger.error("Redis redis error", err.message);
                    return false;
                }
            }
        },
        
        /**
         * Get whole context 
         * 
         * @param {String} contextId
         * 
         * @returns {Object} context
         */
        get: {
            params: {
                contextId: { type: "string" }
            },
            async handler(ctx) {
                // HGETALL
                try {
                    let values = await this.client.hgetall(ctx.params.contextId);
                    for (let key in values) {
                        if (values.hasOwnProperty(key)) {
                            values[key] = await this.serializer.deserialize(values[key]);
                        }
                    }
                    
                    return values;
                } catch (err) /* istanbul ignore next */ {
                    this.logger.error("Redis redis error", err.message);
                    return {};
                }
            }
        },
        
        /**
         * Delete key again from context 
         * 
         * @param {String} contextId
         * @param {String} key
         * 
         * @returns {Boolean} result
         */
        rollback: {
            params: {
                contextId: { type: "string" },
                key: { type: "string" }
            },
            async handler(ctx) {
                // HDEL 
                try {
                    await this.client.hdel(ctx.params.contextId, ctx.params.key);
                    return true;
                } catch (err) /* istanbul ignore next */ {
                    this.logger.error("Redis redis error", err.message);
                    return false;
                }
            }
        }
        
    },

    /**
     * Events
     */
    events: {},

    /**
     * Methods
     */
    methods: {
        
        connect () {
            return new Promise((resolve, reject) => {
                /* istanbul ignore else */
                let redisOptions = this.settings.redis || {};   // w/o settings the client uses defaults: 127.0.0.1:6379
                this.client = new Redis(redisOptions);

                this.client.on("connect", (() => {
                    this.connected = true;
                    this.logger.info("Connected to Redis");
                    resolve();
                }).bind(this));

                this.client.on("close", (() => {
                    this.connected = false;
                    this.logger.info("Disconnected from Redis");
                }).bind(this));

                this.client.on("error", ((err) => {
                    this.logger.error("Redis redis error", err.message);
                    this.logger.debug(err);
                    /* istanbul ignore else */
                    if (!this.connected) reject(err);
                }).bind(this));
            });
        },
        
        async disconnect () {
            return new Promise((resolve) => {
                /* istanbul ignore else */
                if (this.client && this.connected) {
                    this.client.on("close", () => {
                        resolve();
                    });
                    this.client.disconnect();
                } else {
                    resolve();
                }
            });
        }
        
    },

    /**
     * Service created lifecycle event handler
     */
    created() {

        this.serializer = new Serializer();
        
    },

    /**
     * Service started lifecycle event handler
     */
    async started() {

        // connect to redis db
        await this.connect();
        
    },

    /**
     * Service stopped lifecycle event handler
     */
    async stopped() {
        
        // disconnect from redis db
        await this.disconnect();
        
    }

};