# imicros-flow-context
[![Coverage Status](https://coveralls.io/repos/github/al66/imicros-flow-context/badge.svg?branch=master)](https://coveralls.io/github/al66/imicros-flow-context?branch=master)

[Moleculer](https://github.com/moleculerjs/moleculer) service for process instances context store

All values are encrypted before stored in cassandra database.

## Installation
```
$ npm install imicros-flow-context --save
```
## Dependencies / Requirements
Requires broker middleware AclMiddleware or similar (usage of AclMixin):
- [imicros-acl](https://github.com/al66/imicros-acl)

Reuires a running key server for retrieving the owner encryption keys:
- [imicros-keys](https://github.com/al66/imicros-keys)

Requires a running cassandra node/cluster.

# Usage
```js
const { ServiceBroker } = require("moleculer");
const { Context } = require("imicros-flow-context");

broker = new ServiceBroker({
    logger: console
});
broker.createService(Streams, Object.assign({ 
    settings: { 
        cassandra: {
            contactPoints: process.env.CASSANDRA_CONTACTPOINTS || "127.0.0.1", 
            keyspace: process.env.CASSANDRA_KEYSPACE || "imicros"
        }
    }
}));
broker.start();
```
## Actions
- add { instance, key, value } => { true|false }  
- get { instance, key } => { value }  
- remove { instance, key } => { true|false }

