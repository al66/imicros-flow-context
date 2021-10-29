/**
 * @license MIT, imicros.de (c) 2019 Andreas Leinen
 */
"use strict";

const Cassandra = require("cassandra-driver");
const crypto = require("crypto");
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
            contactPoints: process.env.CASSANDRA_CONTACTPOINTS || "127.0.0.1", 
            datacenter: process.env.CASSANDRA_DATACENTER || "datacenter1", 
            keyspace: process.env.CASSANDRA_KEYSPACE_FLOW || "imicros_flow",
            contextTable: "context",
            instanceTable: "instances",
            tokenTable: "token"
        }
        services: {
            keys: "keys"
        }
        */
    },
    $secureSettings: ["cassandra.user","cassandra.password"],

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
         * @param {String} instanceId - uuid
         * @param {String} key  -       PARENT = instance of calling process   or
         *                              START = inital event   or
         *                              process step in format <process>.<step>.<cycle>
         * @param {String} value -      Key PARENT: instance
         *                              Key START: payload of initial event
         *                              Key processs step: result of the executed process stap  
         * 
         * @returns {Boolean} result
         */
        add: {
            acl: "before",
            params: {
                instanceId: { type: "uuid" },
                key: { type: "string" },
                value: { type: "any" }
            },
            async handler(ctx) {
                let owner = ctx.meta?.ownerId ?? null;

                let oek;
                // get owner's encryption key
                try {
                    oek = await this.getKey({ ctx: ctx });
                } catch (err) {
                    throw new Error("failed to receive encryption keys");
                }
                
                let value = await this.serializer.serialize(ctx.params.value);                
                // encrypt value
                let iv = crypto.randomBytes(this.encryption.ivlen);
                try {
                    // hash encription key with iv
                    let key = crypto.pbkdf2Sync(oek.key, iv, this.encryption.iterations, this.encryption.keylen, this.encryption.digest);
                    // encrypt value
                    value = this.encrypt({ value: value, secret: key, iv: iv });
                } catch (err) {
                    this.logger.error("Failed to encrypt value", { 
                        error: err, 
                        iterations: this.encryption.iterations, 
                        keylen: this.encryption.keylen,
                        digest: this.encryption.digest
                    });
                    throw new Error("failed to encrypt");
                }
                
                let query = "INSERT INTO " + this.contextTable + " (owner,instance,key,value,oek,iv) VALUES (:owner,:instance,:key,:value,:oek,:iv);";
                let params = { 
                    owner: owner, 
                    instance: ctx.params.instanceId, 
                    key: ctx.params.key,
                    value : value,
                    oek: oek.id,
                    iv: iv.toString("hex")
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
            acl: "before",
            params: {
                instanceId: { type: "uuid" },
                key: { type: "string" }
            },
            async handler(ctx) {
                let owner = ctx.meta?.ownerId ?? null;

                let query = "SELECT owner, instance, key, value, oek, iv FROM " + this.contextTable;
                query += " WHERE owner = :owner AND instance = :instance AND key = :key;";
                let params = { 
                    owner: owner, 
                    instance: ctx.params.instanceId, 
                    key: ctx.params.key
                };
                try {
                    let result = await this.cassandra.execute(query, params, { prepare: true });
                    let row = result.first();
                    if (row) {

                        let oekId = row.get("oek");
                        let iv = Buffer.from(row.get("iv"), "hex");
                        let encrypted = row.get("value");
                        let value = null;
                        
                        // get owner's encryption key
                        let oek;
                        try {
                            oek = await this.getKey({ ctx: ctx, id: oekId });
                        } catch (err) {
                            this.logger.Error("Failed to retrieve owner encryption key", { owner: owner, key: oekId });
                            throw new Error("failed to retrieve owner encryption key");
                        }

                        // decrypt value
                        try {
                            // hash received key with salt
                            let key = crypto.pbkdf2Sync(oek.key, iv, this.encryption.iterations, this.encryption.keylen, this.encryption.digest);
                            value = this.decrypt({ encrypted: encrypted, secret: key, iv: iv });
                        } catch (err) {
                            throw new Error("failed to decrypt");
                        }
                        
                        // deserialize value
                        value = await this.serializer.deserialize(value);
                        return value;
                    } else {
                        this.logger.info("Unvalid or empty result", { result, first: row, query, params });
                        return null;
                    }
                } catch (err) /* istanbul ignore next */ {
                    this.logger.error("Cassandra select error", { error: err.message, query });
                    return {};
                }
            }
        },

        /**
         * Get payload from context for specified keys
         * 
         * @param {String} instance
         * @param {Array} keys
         * 
         * @returns {Object} key: value
         */
        getKeys: {
            acl: "before",
            params: {
                instanceId: { type: "uuid" },
                keys: { type: "array", item: "string" }
            },
            async handler(ctx) {
                let result = {};
                if (ctx.params.keys.length > 0) {
                    let keys = ctx.params.keys;
                    if (!Array.isArray(keys)) keys = [keys]; 
                    try {
                        for (let i = 0; i < keys.length; i++ ) {
                            let params = {
                                instanceId: ctx.params.instanceId,
                                key: keys[i]
                            };
                            let single = await this.actions.get(params, { parentCtx: ctx, meta: ctx.meta });
                            result[keys[i]] = single;

                        }
                        return result;
                    } catch (err) /* istanbul ignore next */ {
                        return result;
                    }
                } else {
                    let owner = ctx.meta?.ownerId ?? null;

                    let query = "SELECT owner, instance, key FROM " + this.contextTable;
                    query += " WHERE owner = :owner AND instance = :instance;";
                    let params = { 
                        owner: owner, 
                        instance: ctx.params.instanceId 
                    };
                    try {
                        let result = [];
                        let resultSet = await this.cassandra.execute(query, params, {prepare: true});
                        for (const row of resultSet) {
                            result.push({
                                ownerId: row["owner"].toString(),
                                instanceId: row["instance"].toString(),
                                key: row["key"]
                            });
                        }
                        return result;
                    } catch (err) /* istanbul ignore next */ {
                        this.logger.error("Cassandra select error", { error: err.message, query: query, params: params });
                        return false;
                    }
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
            acl: "before",
            params: {
                instanceId: { type: "uuid" },
                key: { type: "string" }
            },
            async handler(ctx) {
                let owner = ctx.meta?.ownerId ?? null;

                let query = "DELETE FROM " + this.contextTable;
                query += " WHERE owner = :owner AND instance = :instance AND key = :key;";
                let params = { 
                    owner: owner, 
                    instance: ctx.params.instanceId, 
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
        },
        
        /**
         * Save token of context 
         * 
         * @param {String} process ID
         * @param {String} instance ID
         * @param {String} element ID
         * @param {Object} token
         * 
         * @returns {Boolean} result
         */
        saveToken: {
            acl: "before",
            params: {
                processId: { type: "uuid" },
                instanceId: { type: "uuid" },
                elementId: { type: "uuid", optional: true },
                token: { type: "object" }
            },
            async handler(ctx) {
                let owner = ctx.meta?.ownerId ?? null;

                let query;
                let params = { 
                    owner: owner, 
                    instance: ctx.params.instanceId ,
                    element: ctx.params.elementId || ctx.params.instanceId
                };
                let token = await this.serializer.serialize(ctx.params.token);
                try {
                    query = `UPDATE ${this.keyspace}.${this.tokenTable} `;
                    query += " SET last = '" + token + "', tokens = tokens + {";
                    query += "'" + token + "'";
                    query += "} WHERE owner = :owner AND instance = :instance AND element = :element ";
                    await this.cassandra.execute(query, params, {prepare: true});
                    this.logger.debug("update token", { query: query });
                    return true;
                } catch (err) /* istanbul ignore next */ {
                    this.logger.error("Cassandra update error", { error: err.message, query: query, params: params });
                    return false;
                }
                       
            }
        },
        
        /**
         * Remove token of context 
         * 
         * @param {String} process ID
         * @param {String} instance ID
         * @param {String} element ID
         * @param {Object} token
         * 
         * @returns {Boolean} result
         */
        removeToken: {
            acl: "before",
            params: {
                processId: { type: "uuid" },
                instanceId: { type: "uuid" },
                elementId: { type: "uuid", optional: true },
                token: { type: "object" }
            },
            async handler(ctx) {
                let owner = ctx.meta?.ownerId ?? null;

                let query;
                let params = { 
                    owner: owner, 
                    instance: ctx.params.instanceId, 
                    element: ctx.params.elementId || ctx.params.instanceId
                };
                let token = await this.serializer.serialize(ctx.params.token);
                try {
                    query = `UPDATE ${this.keyspace}.${this.tokenTable} `;
                    query += " SET tokens = tokens - {";
                    query += "'" + token + "'";
                    query += "} WHERE owner = :owner AND instance = :instance AND element = :element ";
                    await this.cassandra.execute(query, params, {prepare: true});
                    this.logger.debug("update token", { query, params });
                    return true;
                } catch (err) /* istanbul ignore next */ {
                    this.logger.error("Cassandra update error", { error: err.message, query, params });
                    return false;
                }
                       
            }
        },
        
        /**
         * Get tokens from context 
         * 
         * @returns {Array} token
         */
        getToken: {
            acl: "before",
            params: {
                processId: { type: "uuid" },
                instanceId: { type: "uuid" },
                elementId: { type: "uuid", optional: true }
            },
            async handler(ctx) {
                let owner = ctx.meta?.ownerId ?? null;

                let query = `SELECT last, tokens FROM ${this.keyspace}.${this.tokenTable} `;
                query += " WHERE owner = :owner AND instance = :instance AND element = :element ";
                let params = { 
                    owner: owner, 
                    instance: ctx.params.instanceId, 
                    element: ctx.params.elementId  || ctx.params.instanceId
                };
                try {
                    let result = await this.cassandra.execute(query, params, {prepare: true});
                    
                    let row = result.first();
                    if (row) {
                        let lastRaw = row.get("last");
                        let tokensRaw = row.get("tokens");
                        let result = {
                            last: lastRaw ? await this.serializer.deserialize(lastRaw) : null,
                            token: []
                        };
                        if (tokensRaw && Array.isArray(tokensRaw)) {
                            await Promise.all(tokensRaw.map(async (e) => { result.token.push(await this.serializer.deserialize(e)); }));
                        }
                        return result;
                    }
                    return { last: null, token: [] };
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
        
        async getKey ({ ctx = null, id = null } = {}) {
            
            let result = {};
            
            // try to retrieve from keys service
            let opts;
            if ( ctx ) opts = { meta: ctx.meta };
            let params = { 
                service: this.name
            };
            if ( id ) params.id = id;
            
            // call key service and retrieve keys
            try {
                result = await this.broker.call(this.services.keys + ".getOek", params, opts);
                this.logger.debug("Got key from key service", { id: id });
            } catch (err) {
                this.logger.error("Failed to receive key from key service", { id: id, meta: ctx.meta });
                throw err;
            }
            if (!result.id || !result.key) throw new Error("Failed to receive key from service", { result: result });
            return result;
        },
        
        encrypt ({ value = ".", secret, iv }) {
            let cipher = crypto.createCipheriv("aes-256-cbc", secret, iv);
            let encrypted = cipher.update(value, "utf8", "hex");
            encrypted += cipher.final("hex");
            return encrypted;
        },

        decrypt ({ encrypted, secret, iv }) {
            let decipher = crypto.createDecipheriv("aes-256-cbc", secret, iv);
            let decrypted = decipher.update(encrypted, "hex", "utf8");
            decrypted += decipher.final("utf8");
            return decrypted;            
        },
        
        async connect () {

            // connect to cassandra cluster
            await this.cassandra.connect();
            this.logger.info("Connected to cassandra", { contactPoints: this.contactPoints, datacenter: this.datacenter, keyspace: this.keyspace });
            
            // create tables, if not exists
            let query = `CREATE TABLE IF NOT EXISTS ${this.keyspace}.${this.contextTable} `;
            query += " ( owner varchar, instance uuid, key varchar, value varchar, oek uuid, iv varchar, PRIMARY KEY (owner,instance,key) ) ";
            query += " WITH comment = 'storing process context';";
            await this.cassandra.execute(query);

            query = `CREATE TABLE IF NOT EXISTS ${this.keyspace}.${this.instanceTable} `;
            query += " ( owner varchar, process uuid, instance uuid, created timestamp, completed timestamp, tokens set<text>, PRIMARY KEY (owner,process,instance) ) ";
            query += " WITH comment = 'storing process instances';";
            await this.cassandra.execute(query);

            query = `CREATE TABLE IF NOT EXISTS ${this.keyspace}.${this.tokenTable} `;
            query += " ( owner varchar, instance uuid, element uuid, last varchar, tokens set<text>, PRIMARY KEY (owner,instance,element) ) ";
            query += " WITH comment = 'storing instance token';";
            await this.cassandra.execute(query);

        },
        
        async disconnect () {

            // close all open connections to cassandra
            await this.cassandra.shutdown();
            this.logger.info("Disconnected from cassandra", { contactPoints: this.contactPoints, datacenter: this.datacenter, keyspace: this.keyspace });
            
        }
        
    },

    /**
     * Service created lifecycle event handler
     */
    async created() {

        // set actions
        this.services = {
            keys: this.settings?.services?.keys ?? "keys"
        };        
        
        // encryption setup
        this.encryption = {
            iterations: 1000,
            ivlen: 16,
            keylen: 32,
            digest: "sha512"
        };
        
        this.serializer = new Serializer();

        // cassandra setup
        this.contactPoints = ( this.settings?.cassandra?.contactPoints ?? "127.0.0.1" ).split(",");
        this.datacenter = this.settings?.cassandra?.datacenter ?? "datacenter1";
        this.keyspace = this.settings?.cassandra?.keyspace ?? "imicros_flow";
        this.contextTable = this.settings?.cassandra?.contextTable ?? "context";
        this.instanceTable = this.settings?.cassandra?.instanceTable ?? "instance";
        this.tokenTable = this.settings?.cassandra?.tokenTable ?? "tokens";
        this.config = {
            contactPoints: this.contactPoints, 
            localDataCenter: this.datacenter, 
            keyspace: this.keyspace, 
            protocolOptions: { 
                port: this.settings?.cassandra?.port ?? (process.env.CASSANDRA_PORT || 9042 )
            },
            credentials: { 
                username: this.settings?.cassandra?.user ?? (process.env.CASSANDRA_USER || "cassandra"), 
                password: this.settings?.cassandra?.password ?? (process.env.CASSANDRA_PASSWORD || "cassandra") 
            }
        };
        this.cassandra = new Cassandra.Client(this.config);

        this.broker.waitForServices(Object.values(this.services));
        
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