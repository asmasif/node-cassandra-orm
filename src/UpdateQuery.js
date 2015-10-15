var _Statement = require('./_Statement.js'),
    _ = require('lodash'),
    Logger = require('../bunyan/Bunyan.js'),
    logger = new Logger('NODE', 'Cassandra'),
    CassandraDB = require('./DBClient.js'),
    cassandraDB = new CassandraDB();

/**
 * Should only be used by the cassandra/BaseModel
 */
function UpdateQuery(keyspace, table, updateRestrictions, hostName) {
    var connection = cassandraDB.getConnection(hostName);
    if (!keyspace || typeof keyspace !== 'string') {
        throw new Error('LIB_CAS_UQ_1');
    }
    if (!table || typeof table !== 'string') {
        throw new Error('LIB_CAS_UQ_2');
    }
    if(!_.isArray(updateRestrictions) && !_.isUndefined(updateRestrictions)){
        throw new Error('LIB_CAS_UQ_3');
    }
    if (_.isArray(updateRestrictions) && _.isEmpty(updateRestrictions)) {
        throw new Error('LIB_CAS_UQ_4');
    }

    var updateStr = '', setStr = '', whereStr = '', optionsStr = '', queryParams=[];

    /**
     * creates the UPDATE query
     * @returns {UpdateQuery}
     */
    function update() {
        updateStr = _Statement.update(keyspace, table);
        return this;
    }

    /**
     * creates the SET part of the query
     * @param nameValueMap
     * @returns {UpdateQuery}
     */
    function set(nameValueMap) {
        if(!_.isPlainObject(nameValueMap) || _.isEmpty(nameValueMap)){
            throw new Error('LIB_CAS_UQ_5');
        }
        setStr = _Statement.set(nameValueMap, queryParams, updateRestrictions);
        return this;
    }

    /**
     * Creates the WHERE part of the query
     * @param clauses
     * @returns {UpdateQuery}
     */
    function where(filters) {
        if(!_.isPlainObject(filters)){
            throw new Error('LIB_CAS_UQ_6');
        }
        whereStr = _Statement.where(filters, queryParams);
        return this;
    }

    /**
     * Adds the USING .. clause
     *
     * @param options
     * @returns {UpdateQuery}
     */
    function usingOptions(options) {
        if (!_.isPlainObject(options)) {
            throw new Error('LIB_CAS_IQ_7');
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
        var query = updateStr;
        if (optionsStr) {
            query += ' ' + optionsStr;
        }
        if (setStr) {
            query += ' ' + setStr;
        }
        if (whereStr) {
            query += ' ' + whereStr;
        }
        return query;
    }

    /**
     * Executes an UPDATE query
     * @param callback
     */
    function execute(callback) {
        var query = toString();
        connection.execute(query, queryParams, {prepare: true}, function (err, response) {
            if (err) {
                logger.error('LIB_CAS_UQ_8', {query: query, queryParams: queryParams, message: err.message});
                callback(err);
                return;
            }
            callback(null, response);
        });
    }

    // public interface
    this.update = update;
    this.set = set;
    this.where = where;
    this.usingOptions = usingOptions;
    this.toString = toString;
    this.execute = execute;
}

module.exports = UpdateQuery;