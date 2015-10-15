/**
 * @fileOverview ReadQuery
 * @class ReadQuery
 * @memberOf cassandra
 * @description Interface for creating Cassandra SELECT queries
 */
var _Statement = require('./_Statement.js');
var _ = require('lodash'),
    Logger = require('../bunyan/Bunyan.js'),
    logger = new Logger('NODE', 'Cassandra'),
    CassandraDB = require('./DBClient.js'),
    cassandraDB = new CassandraDB();

/**
 * Should only be used by the cassandra/BaseModel
 */
function ReadQuery(keyspace, table, readable, hostName) {
    var connection = cassandraDB.getConnection(hostName);
    if (!keyspace || !_.isString(keyspace)) {
        throw new Error('LIB_CAS_RQ_1');
    }
    if (!table || !_.isString(table)) {
        throw new Error('LIB_CAS_RQ_2');
    }
    // if readable is defined, it has to be a non-empty array
    if (!_.isUndefined(readable) && (!_.isArray(readable) || _.isEmpty(readable))) {
        throw new Error('LIB_CAS_RQ_3');
    }

    var selectStr = '', whereStr = '', limitStr = '', orderByStr = '', allowFilteringFlag = false, queryParams = [];

    /**
     * @function select
     * @description Specified which columns to select from the table given in the schema map
     * @memberOf ReadQuery
     * @param {Array} columns is an optional array of strings with the name of the columns to select
     * @return {Class} Returns the ReadQuery object so that function calls can be chained
     */
    function select(columns) {
        if(!_.isArray(columns) && !_.isUndefined(columns)){
            throw new Error('LIB_CAS_RQ_4');
        }
        if(_.isArray(columns) && _.isEmpty(columns)){
            throw new Error('LIB_CAS_RQ_5');
        }

        selectStr = _Statement.select(keyspace, table, columns, readable);
        return this;
    }

    /**
     * @function where
     * @description Adds a WHERE clause for the given operator and key value pair to the SELECT query
     * @memberOf ReadQuery
     * @param {(Array|Object)} WhereClause object or an array of clause objects. Clause object have the form {operator: {key: value}}
     * @return {Class} Returns the ReadQuery object so that function calls can be chained
     */
    function where(filters) {
        if(!_.isPlainObject(filters)){
            throw new Error('LIB_CAS_RQ_6');
        }
        whereStr = _Statement.where(filters, queryParams);
        return this;
    }

    /**
     * @function limit
     * @description Adds a LIMIT clause to the SELECT query
     * @memberOf ReadQuery
     * @param {Integer} Size Size of the limit
     * @return {Class} Returns the ReadQuery object so that function calls can be chained
     */
    function limit(size) {
        if(!_.isNumber(size)){
            throw new Error('LIB_CAS_RQ_7');
        }
        limitStr = _Statement.limit(size);
        return this;
    }

    /**
     * @function orderBy
     * @description Adds an ORDER BY clause to the SELECT query. Defaults to Ascending if no direction is specified
     * @memberOf ReadQuery
     * @param {string} Column Column to sort by
     * @param {string} [direction=ASC] Direction Diretion to sort by. Either ASC or DESC.
     * @return {Class} Returns the ReadQuery object so that function calls can be chained
     */
    function orderBy(key, direction) {
        if(!_.isString(key) || !_.contains(['ASC', 'DESC'], direction)){
            throw new Error('LIB_CAS_RQ_8');
        }
        orderByStr = _Statement.orderBy(key, direction);
        return this;
    }

    /**
     * @function allowFiltering
     * @description Adds ALLOW FILTERING at the end of the query
     * @memberOf ReadQuery
     * @return {Class} Returns the ReadQuery object so that function calls can be chained
     */
    function allowFiltering() {
        allowFilteringFlag = true;
        return this;
    }

    /**
     * @function toString
     * @description Stringifies the ReadQuery.
     * @memberOf ReadQuery
     * @return {string} Returns a string of the current query
     */
    function toString() {
        var query = selectStr;
        if (whereStr) {
            query += ' ' + whereStr;
        }
        if (orderByStr) {
            query += ' ' + orderByStr;
        }
        if (limitStr) {
            query += ' ' + limitStr;
        }
        if (allowFilteringFlag) {
            query += ' ALLOW FILTERING';
        }
        return query;
    }

    /**
     * @function execute
     * @description Executes the ReadQuery.
     * @memberOf ReadQuery
     * @throws LIB_CAS_RQ_1 Must call {@link ReadQuery.select|select} on the ReadQuery before executing
     * @param {Function} callback Callback function
     */
    function execute(callback) {
        var query = toString();
        connection.execute(query, queryParams, {prepare: true}, function (err, response) {
            if (err) {
                logger.error('LIB_CAS_RQ_9', {query: query, queryParams: queryParams, message: err.message});
                callback(err);
                return;
            }
            callback(null, response);
        });
    }

    function stream() {
        var query = toString();
        return connection.stream(query, queryParams, {prepare: true});
    }

    function eachRow(options, process, callback) {
        _.assign(options, {prepare: true});
        var query = toString();
        connection.eachRow(query, queryParams, options, process, callback);
    }

    // public interface
    this.select = select;
    this.where = where;
    this.limit = limit;
    this.orderBy = orderBy;
    this.allowFiltering = allowFiltering;
    this.toString = toString;
    this.execute = execute;
    this.stream = stream;
    this.eachRow = eachRow;
}

module.exports = ReadQuery;