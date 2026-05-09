"use strict";

const cqrs = require("./classes/cqrs/cqrs");
const cassandra = require("./classes/db/cassandraCQRS");
const postgres = require("./classes/db/postgresqlCQRS");

module.exports = {
    ...cqrs,
    ...cassandra,
    ...postgres
};
