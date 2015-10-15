var _Statement = require('./_Statement.js'),
    _ = require('lodash'),
    Logger = require('../bunyan/Bunyan.js'),
    logger = new Logger('NODE', 'Cassandra'),
    CassandraDB = require('./DBClient.js'),
    cassandraDB = new CassandraDB();

/**
 * Should only be used by the cassandra/BaseModel
 */
function DeleteQuery(keyspace, table, hostName) {
    var connection = cassandraDB.getConnection(hostName);
    if (!keyspace || typeof keyspace !== 'string') {
        throw new Error('LIB_CAS_DQ_1');
    }
    if (!table || typeof table !== 'string') {
        throw new Error('LIB_CAS_DQ_2');
    }

    var deleteStr = '', optionsStr = '', whereStr = '', queryParams = [];

    /**
     * creates DELETE statement
     *
     * @param columns
     * @returns {DeleteQuery}
     */
    function delete_() {
        deleteStr = _Statement.delete(keyspace, table);
        return this;
    }

    /**
     * Adds the USING .. clause
     *
     * @param options
     * @returns {DeleteQuery}
     */
    function usingOptions(options) {
        if (_.isEmpty(options) || !_.isPlainObject(options)) {
            throw new Error('LIB_CAS_DQ_5');
        }
        optionsStr = _Statement.usingOptions(options);
        return this;
    }

    /**
     * @function where
     * @description Adds a WHERE clause for the given operator and key value pair to the SELECT query
     * @memberOf ReadQuery
     * @param {(Array|Object)} WhereClause object or an array of clause objects. Clause object have the form {opertator: {key: value}}
     * @return {DeleteQuery} Returns the ReadQuery object so that function calls can be chained
     */
    function where(filters) {
        if(!_.isPlainObject(filters)){
            throw new Error('LIB_CAS_DQ_6');
        }
        whereStr = _Statement.where(filters, queryParams);
        return this;
    }

    /**
     * Stitches different parts of the query and returns the full string
     *
     * @returns {string}
     */
    function toString() {
        var query = deleteStr;
        if (optionsStr) {
            query += ' ' + optionsStr;
        }
        if (whereStr) {
            query += ' ' + whereStr;
        }

        return query;
    }

    /**
     * Executes an DELETE query
     * @param callback
     */
    function execute(callback) {
        var query = toString();
        connection.execute(query, queryParams, {prepare: true}, function (err, response) {
            if (err) {
                logger.error('LIB_CAS_DQ_7', {query: query, queryParams: queryParams, message: err.message});
                callback(err);
                return;
            }
            callback(null, response);
        });
    }

    // public interface
    this.delete = delete_;
    this.usingOptions = usingOptions;
    this.where = where;
    this.toString = toString;
    this.execute = execute;
}

module.exports = DeleteQuery;