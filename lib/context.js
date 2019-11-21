/**
 * @license MIT, imicros.de (c) 2019 Andreas Leinen
 */
"use strict";

const Cassandra = require("cassandra-driver");
const _ = require("lodash");
const { AclMixin } = require("imicros-acl");
const { Serializer } = require("./serializer/base");

module.exports = {
    name: "context",
    mixins: [AclMixin],
    
    /**
     * Service settings
     */
    settings: {
        /*
        cassandra: {
            contactPoints: ["192.168.2.124"],
            datacenter: "datacenter1",
            keyspace: "imicros_flow",
            contextTable: "context",
            instanceTable: "instances"
        }
        */
    },

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
         * @param {String} instance - uuid
         * @param {String} key  -   PARENT = instance of calling process   or
         *                          START = inital event   or
         *                          process step in format <process>.<step>.<cycle>
         * @param {String} value -  Key PARENT: instance
         *                          Key START: payload of initial event
         *                          Key processs step: result of the executed process stap  
         * 
         * @returns {Boolean} result
         */
        add: {
            params: {
                instance: { type: "uuid" },
                key: { type: "string" },
                value: { type: "any" }
            },
            async handler(ctx) {
                let owner = await this.getOwnerId({ ctx: ctx, abort: true });
                if (!await this.isAuthorized({ ctx: ctx, ressource: { instance: ctx.params.instance }, action: "add" })) throw new Error("not authorized");

                let value = await this.serializer.serialize(ctx.params.value);
                let query = "INSERT INTO " + this.contextTable + " (owner,instance,key,value) VALUES (:owner,:instance,:key,:value);";
                let params = { 
                    owner: owner, 
                    instance: ctx.params.instance, 
                    key: ctx.params.key,
                    value : value
                };
                try {
                    await this.cassandra.execute(query, params, {prepare: true});
                    return true;
                } catch (err) /* istanbul ignore next */ {
                    this.logger.error("Cassandra insert error", { error: err.message, query: query, params: params });
                    return false;
                }
            }
        },
        
        /**
         * Get payload from context 
         * 
         * @param {String} instance
         * @param {String} key
         * 
         * @returns {Object} value
         */
        get: {
            params: {
                instance: { type: "uuid" },
                key: { type: "string" }
            },
            async handler(ctx) {
                let owner = await this.getOwnerId({ ctx: ctx, abort: true });
                if (!await this.isAuthorized({ ctx: ctx, ressource: { instance: ctx.params.instance }, action: "get" })) throw new Error("not authorized");

                let query = "SELECT owner, instance, key, value FROM " + this.contextTable;
                query += " WHERE owner = :owner AND instance = :instance AND key = :key;";
                let params = { 
                    owner: owner, 
                    instance: ctx.params.instance, 
                    key: ctx.params.key
                };
                try {
                    let result = await this.cassandra.execute(query, params, { prepare: true });
                    let row = result.first();
                    if (row) {
                        let value = await this.serializer.deserialize(row.get("value"));
                        return value;
                    } else {
                        this.logger.info("Unvalid or empty result", { result: result, first: row, query: query });
                        return null;
                    }
                } catch (err) /* istanbul ignore next */ {
                    this.logger.error("Cassandra select error", { error: err.message, query: query });
                    return {};
                }
            }
        },
        
        /**
         * Remove key from context 
         * 
         * @param {String} instance
         * @param {String} key
         * 
         * @returns {Boolean} result
         */
        remove: {
            params: {
                instance: { type: "uuid" },
                key: { type: "string" }
            },
            async handler(ctx) {
                let owner = await this.getOwnerId({ ctx: ctx, abort: true });
                if (!await this.isAuthorized({ ctx: ctx, ressource: { instance: ctx.params.instance }, action: "remove" })) throw new Error("not authorized");

                let query = "DELETE FROM " + this.contextTable;
                query += " WHERE owner = :owner AND instance = :instance AND key = :key;";
                let params = { 
                    owner: owner, 
                    instance: ctx.params.instance, 
                    key: ctx.params.key
                };
                try {
                    await this.cassandra.execute(query, params, {prepare: true});
                    return true;
                } catch (err) /* istanbul ignore next */ {
                    this.logger.error("Cassandra insert error", { error: err.message, query: query, params: params });
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
        
        async connect () {

            // connect to cassandra cluster
            await this.cassandra.connect();
            this.logger.info("Connected to cassandra", { contactPoints: this.contactPoints, keyspace: this.keyspace });
            
            // validate parameters
            // TODO! pattern doesn't work...
            let params = {
                keyspace: this.keyspace, 
                tablename: this.contextTable
            };
            let schema = {
                keyspace: { type: "string", trim: true },
                //tablename: { type: "string", trim: true, pattern: "[a-z][a-z0-9]*(_[a-z0-9]+)*", patternFlags: "g" } // doesn't work
                //tablename: { type: "string", trim: true, pattern: /[a-z][a-z0-9]*(_[a-z0-9]+)*/ } // doesn't work
                tablename: { type: "string", trim: true }
            };
            let valid = await this.broker.validator.validate(params,schema);
            if (!valid) {
                this.logger.error("Validation error", { params: params, schema: schema });
                throw new Error("Unalid table parameters. Cannot init cassandra database.");
            }
            
            // create tables, if not exists
            let query = `CREATE TABLE IF NOT EXISTS ${this.keyspace}.${this.contextTable} `;
            query += " ( owner varchar, instance uuid, key varchar, value varchar, PRIMARY KEY (owner,instance,key) ) ";
            query += " WITH comment = 'storing process context';";
            await this.cassandra.execute(query);

            query = `CREATE TABLE IF NOT EXISTS ${this.keyspace}.${this.instanceTable} `;
            query += " ( owner varchar, process uuid, instance uuid, created timestamp, completed timestamp, PRIMARY KEY (owner,process,instance) ) ";
            query += " WITH comment = 'storing process instances';";
            await this.cassandra.execute(query);

        },
        
        async disconnect () {

            // close all open connections to cassandra
            await this.cassandra.shutdown();
            this.logger.info("Disconnected from cassandra", { hosts: this.contactPoints, keyspace: this.keyspace });
            
        }
        
    },

    /**
     * Service created lifecycle event handler
     */
    async created() {

        this.serializer = new Serializer();
        //this.contactPoints = ["192.168.2.124"];
        this.contactPoints = _.get(this.settings, "cassandra.contactPoints", "127.0.0.1" ).split(",");
        this.datacenter = "datacenter1",
        this.keyspace = "imicros_flow";
        this.contextTable = "context";
        this.instanceTable = "instance";
        this.cassandra = new Cassandra.Client({ contactPoints: this.contactPoints, localDataCenter: this.datacenter, keyspace: this.keyspace });
        
    },

    /**
     * Service started lifecycle event handler
     */
    async started() {

        // connect to db
        await this.connect();
        
    },

    /**
     * Service stopped lifecycle event handler
     */
    async stopped() {
        
        // disconnect from db
        await this.disconnect();
        
    }

};