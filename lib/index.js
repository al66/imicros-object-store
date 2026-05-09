"use strict";

const cqrs = require("./classes/cqrs/cqrs");
const cassandra = require("./classes/db/cassandraCQRS");

module.exports = {
    ...cqrs,
    ...cassandra
};
