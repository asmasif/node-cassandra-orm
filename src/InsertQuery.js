var _Statement = require('./_Statement.js'),
    _ = require('lodash'),
    Logger = require('../bunyan/Bunyan.js'),
    logger = new Logger('NODE', 'Cassandra'),
    CassandraDB = require('./DBClient.js'),
    cassandraDB = new CassandraDB();

/**
 * Should only be used by the cassandra/BaseModel
 */
function InsertQuery(keyspace, table, hostname) {
    var connection = cassandraDB.getConnection(hostname);
    if (!keyspace || typeof keyspace !== 'string') {
        throw new Error('LIB_CAS_IQ_1');
    }
    if (!table || typeof table !== 'string') {
        throw new Error('LIB_CAS_IQ_2');
    }

    var insertValuesStr = '', optionsStr = '', queryParams = [];

    /**
     * creates INSERT INTO ... (..) VALUES (..) statement
     *
     * @param insertMap
     * @returns {InsertQuery.insertValues}
     */
    function insertValues(insertMap) {
        if (!_.isPlainObject(insertMap) || _.isEmpty(insertMap)) {
            throw new Error('LIB_CAS_IQ_3');
        }

        insertValuesStr = _Statement.insertValues(keyspace, table, insertMap, queryParams);
        return this;
    }

    /**
     * Adds the USING .. clause
     *
     * @param options
     * @returns {InsertQuery.usingOptions}
     */
    function usingOptions(options) {
        if (_.isEmpty(options) || !_.isPlainObject(options)) {
            throw new Error('LIB_CAS_IQ_6');
        }
        optionsStr = _Statement.usingOptions(options);
        return this;
    }

    /**
     * Stitches different parts of the query and returns the full string
     *
     * @returns {string}
     */
    function toString() {
        var query = insertValuesStr;
        if (optionsStr) {
            query += ' ' + optionsStr;
        }
        return query;
    }

    /**
     * Executes an INSERT query
     * @param callback
     */
    function execute(callback) {
        var query = toString();
        connection.execute(query, queryParams, {prepare: true}, function (err, response) {
            if (err) {
                logger.error('LIB_CAS_IQ_7', {query: query, queryParams: queryParams, message: err.message});
                callback(err);
                return;
            }
            callback(null, response);
        });
    }

    this.insertValues = insertValues;
    this.usingOptions = usingOptions;
    this.toString = toString;
    this.execute = execute;
}

module.exports = InsertQuery;