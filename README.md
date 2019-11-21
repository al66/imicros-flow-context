# imicros-context
[![Build Status](https://travis-ci.org/al66/imicros-context.svg?branch=master)](https://travis-ci.org/al66/imicros-context)
[![Coverage Status](https://coveralls.io/repos/github/al66/imicros-context/badge.svg?branch=master)](https://coveralls.io/github/al66/imicros-context?branch=master)

[Moleculer](https://github.com/moleculerjs/moleculer) service for process instances context store

## Installation
```
$ npm install imicros-context --save
```
## Dependencies
Requires middleware AclMiddleware or similar (use of AclMixin):
- [imicros-acl](https://github.com/al66/imicros-acl)

Requires a running cassandra node/cluster.

# Usage
```js
const { ServiceBroker } = require("moleculer");
const { Context } = require("imicros-context");

broker = new ServiceBroker({
    logger: console
});
broker.createService(Streams, Object.assign({ 
    settings: { 
        cassandra: {
            contactPoints: process.env.CASSANDRA_CONTACTPOINTS || "127.0.0.1" 
        }
    }
}));
broker.start();
```
## Actions
- add { instance, key, value } => { true|false }  
- get { instance, key } => { value }  
- remove { instance, key } => { true|false }

