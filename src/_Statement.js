//Base Statement
var _ = require('lodash');
var util = require('../Util.js');

/**
 * Should only be used by the private _Query classes
 */
var _Statement = (function () {

    var operatorMap = {
        eq: '=',
        lt: '<',
        lte: '<=',
        gt: '>',
        gte: '>=',
        'in': 'IN'
    };

    /**
     * Create the SELECT .... FROM .. part of the query, while checking request validity
     *
     * @param {string} keyspace
     * @param {string} table
     * @param {string[]} columns (optional)
     * @param {string[]} readable (optional)
     * @returns {string}
     */
    function select(keyspace, table, columns, readable) {

        var query = 'SELECT';

        if (!_.isArray(columns) && !_.isUndefined(columns)) {
            throw new Error('LIB_CAS__ST_1');
        }
        if(_.isArray(columns) && _.isEmpty(columns) ){
            throw new Error('LIB_CAS__ST_2');
        }

        if (!_.isArray(readable) && !_.isUndefined(readable)) {
            throw new Error('LIB_CAS__ST_3');
        }
        if(_.isArray(readable) && _.isEmpty(readable)){
            throw new Error('LIB_CAS__ST_4');
        }

        if(_.isUndefined(columns)){
            // select all readable columns
            query += ' ' + (_.isArray(readable) ? readable.join(', ') : '*');
        } else {
            // ensure columns is a subset of readable
            if (requestedInvalidColumns(readable, columns)) {
                throw new Error('LIB_CAS__ST_5');
            }
            query += ' ' + columns.join(', ');
        }

        query += ' FROM ' + keyspace + '.' + table;
        return query;
    }

    /**
     * Checks whether any invalid or inaccessible column was requested
     *
     * @param readable
     * @param columns
     * @returns {boolean}
     */
    function requestedInvalidColumns(readable, columns) {
        if(!_.isArray(readable)){
            return false;
        } else {
            return !_.isEmpty(_.difference(columns, readable));
        }
    }

    /**
     * create a clause list from a filters object
     *
     * @param filters
     * @returns {object}
     */
    function createClausesFromFilters(filters) {
        var clauses = [];
        // if filters are passed, check if they are part of primary or secondary
        _.forOwn(filters, function (val, key) {
            var keyVal = {};
            if (Array.isArray(val)) {
                    keyVal[key] = val;
                    clauses.push({in: keyVal});
            } else if (_.isPlainObject(val)) {
                _.forOwn(val, function (clauseVal, clauseOp) {
                    var clause = {};
                    keyVal[key] = clauseVal;
                    clause[clauseOp] = _.clone(keyVal);
                    clauses.push(clause);
                });
            } else {
                keyVal[key] = val;
                clauses.push({
                    eq: keyVal
                });
            }
        });
        return clauses;
    }

    /**
     * Creates the entire where clause string 'WHERE ... AND ...'
     *
     * @param filters
     * @param queryParams
     * @returns {string}
     */
    function where(filters, queryParams) {

        if (!_.isPlainObject(filters)) {
            throw new Error('LIB_CAS__ST_6');
        }

        var clauses = createClausesFromFilters(filters);
        if(_.isEmpty(clauses)){
            return '';
        }

        // if only one clause is passed
        if (!_.isArray(clauses)) {
            return 'WHERE ' + createClauseString(clauses, queryParams);
        }

        // for more than one clause, always join by AND since cassandra only supports AND
        var clausesArray = [];
        for (var i = 0, limit = clauses.length; i < limit; i += 1) {
            clausesArray.push(createClauseString(clauses[i], queryParams));
        }
        return 'WHERE ' + clausesArray.join(' AND ');
    }

    /**
     * This is prefix to infix convertion, given a clause object {op:{key:value}} and field definitions,
     * checks validity and creates 'key op value'
     *
     * @param clauseObj
     * @param queryParams
     * @returns {string}
     */
    function createClauseString(clauseObj, queryParams) {

        var keys = Object.keys(clauseObj);

        var operator = operatorMap[keys[0]];

        var nameValue = clauseObj[keys[0]];

        // make sure there is only one key value pair
        keys = Object.keys(nameValue);

        //Get the key, value and columns type
        var field = keys[0];
        var value = nameValue[keys[0]];

        // if not validate
        if (operator !== 'IN') {
            queryParams.push(value);
            return field + ' ' + operator + ' ?';
        }

        var valueArr = [];
        for (var i = 0, limit = value.length; i < limit; i += 1) {
            queryParams.push(value[i]);
            valueArr.push('?');
        }
        return field + ' ' + operator + ' (' + valueArr.join(', ') + ')';
    }

    /**
     * Validate and then create the ORDER BY clause
     *
     * @param key
     * @param direction
     * @returns {string}
     */
    function orderBy(key, direction) {

        // direction must be ASC or DESC
        if (direction !== 'ASC' && direction !== 'DESC') {
            throw new Error('LIB_CAS__ST_14');
        }

        return 'ORDER BY ' + key + ' ' + direction;
    }

    /**
     * Returns the LIMIT clause
     * @param size
     * @returns {string}
     */
    function limit(size) {
        // throw an error if the size is not an integer or if it is negative
        if (parseInt(size, 10) + '' !== size + '' || parseInt(size, 10) < 1) {
            throw new Error('LIB_CAS__ST_15');
        }
        return 'LIMIT ' + size;
    }

    /**
     * Return the INSERT INTO keyspace.table (...) VALUES (...) clause
     *
     * @param keyspace
     * @param table
     * @param insertMap
     * @param queryParams
     * @returns {string}
     */
    function insertValues(keyspace, table, insertMap, queryParams) {

        if(!_.isPlainObject(insertMap) || _.isEmpty(insertMap)){
            throw new Error('LIB_CAS__ST_16');
        }

        var values = [];
        var columns = [];

        var query = 'INSERT INTO ' + keyspace + '.' + table + ' (';

        _.forEach(insertMap, function(val, key){
            columns.push(key);
            queryParams.push(val);
            values.push('?');
        });

        return query + columns.join(', ') + ') VALUES (' + values.join(', ') + ')';
    }

    /**
     * Returns the USING.. part of the cassandra query
     *
     * @param options
     * @returns {string}
     */
    function usingOptions(options) {
        // options need to be a non-empty {}
        if (_.isEmpty(options) || !_.isPlainObject(options)) {
            throw new Error('LIB_CAS__ST_21');
        }

        var optionsArray = [];
        _.forOwn(options, function (value, key) {
            key = (key + '').toUpperCase();
            if (['TTL', 'TIMESTAMP'].indexOf(key) === -1) {
                throw new Error('LIB_CAS__ST_22');
            }
            optionsArray.push(key + ' ' + value);
        });
        return 'USING ' + optionsArray.join(' AND ');
    }

    /**
     * returns UPDATE keyspace.table .. part of the query
     *
     * @param keyspace
     * @param table
     * @returns {string}
     */
    function update(keyspace, table) {
        return 'UPDATE ' + keyspace + '.' + table;
    }

    /**
     * returns SET key = value
     *
     * @param nameValueMap
     * @returns {string}
     */
    function set(nameValueMap, queryParams, updateRestrictions) {

        if (arguments.length < 2) {
            throw new Error('LIB_CAS__ST_31');
        }

        // nameValueMap need to be a non-empty {}
        if (_.isEmpty(nameValueMap) || !_.isPlainObject(nameValueMap)) {
            throw new Error('LIB_CAS__ST_23');
        }

        var setClauses = [];
        _.forOwn(nameValueMap, function (value, name) {
            if (_.isArray(updateRestrictions) && !_.contains(updateRestrictions, name)) {
                throw new Error('LIB_CAS__ST_26');
            }
            queryParams.push(value);
            setClauses.push(name + ' = ?');
        });

        return 'SET ' + setClauses.join(', ');
    }

    /**
     * returns DELETE statement, columns are optional
     *
     * @param keyspace
     * @param table
     * @returns {string}
     */
    function delete_(keyspace, table) {
        return 'DELETE FROM ' + keyspace + '.' + table;
    }

    // public interface
    return {
        select: select,
        where: where,
        orderBy: orderBy,
        limit: limit,
        insertValues: insertValues,
        usingOptions: usingOptions,
        update: update,
        set: set,
        delete: delete_
    };
})();

module.exports = _Statement;