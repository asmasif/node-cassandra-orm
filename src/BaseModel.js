/**
 * @fileOverview ReadQuery
 * @class CRUDModel
 * @memberOf internalDashboard
 * @description CRUD Model
 */
var _ = require('lodash');
var Util = require('./../Util.js');
var ReadQuery = require('./ReadQuery.js');
var InsertQuery = require('./InsertQuery.js');
var UpdateQuery = require('./UpdateQuery.js');
var DeleteQuery = require('./DeleteQuery.js');
var Long = require('cassandra-driver').types.Long;
var Uuid = require('cassandra-driver').types.Uuid;
function BaseModel(schemaMap, hostName) {

    var keyspace = schemaMap.keyspace,
        table = schemaMap.table,
        readable = schemaMap.readable,
        writable = schemaMap.writable;

    /**
     * @function validateSchemaMap
     * @description Validate schema map of the model
     */
    function validateSchemaMap() {
        // constructor code that validates schemaMap
        if (!keyspace || typeof keyspace !== 'string') {
            throw new Error('LIB_CAS_BM_1');
        }

        if (!table || typeof table !== 'string') {
            throw new Error('LIB_CAS_BM_2');
        }

        if (!_.isArray(readable) && !_.isUndefined(readable)) {
            throw new Error('LIB_CAS_BM_3');
        }
        if (!_.isArray(writable) && !_.isUndefined(writable)) {
            throw new Error('LIB_CAS_BM_4');
        }
    }

    /**
     * @function getSchemaMap
     * @description Get schema map of the model
     * @param {boolean} getReference Determines whether to get reference or value
     * @returns {*}
     */
    function getSchemaMap(getReference) {
        if (getReference) {
            return schemaMap;
        }
        return _.cloneDeep(schemaMap);
    }

    function setKeyspace(ks) {
        keyspace = ks;
        schemaMap.keyspace = ks;
    }

    /**
     * @function find
     * @description Finds a list of matching rows
     * @memberOf BaseModel
     * @access public
     * @param arg1
     * @param arg2
     * @param arg3
     * @return {Array} Returns an array of Row objects
     */
    function find(arg1, arg2, arg3) {
        //add optional parameter postProcess that postProcesses the response data values from bigint/uuid objects to flat data
        var filters = {},
            options = {},
            callback = null;
        // throw error if no argument was provided
        if (arguments.length === 0) {
            throw new Error('LIB_CAS_BM_5');
        }

        if (arguments.length === 1) {
            callback = arg1;
        } else if (arguments.length === 2) {
            filters = arg1;
            callback = arg2;
        } else {
            filters = arg1;
            options = arg2;
            callback = arg3;
        }

        if (!_.isPlainObject(filters)) {
            throw new Error('LIB_CAS_BM_6');
        }
        if (!_.isPlainObject(options)) {
            throw new Error('LIB_CAS_BM_7');
        }

        var query = new ReadQuery(keyspace, table, readable, hostName);

        var columns = options.columns;
        query.select(columns);

        query.where(filters);


        if (options.limit) {
            query.limit(options.limit);
        }

        if (options.sortBy) {
            query.orderBy(options.sortBy, (options.sortOrder ? options.sortOrder : 'ASC'));
        }

        if (options.allowFiltering) {
            query.allowFiltering();
        }

        query.execute(function (err, response) {
            if (err) {
                callback(err);
                return;
            }
            if (options.postProcess) {
                for (var i = 0; i < response.rows.length; i++) {
                    var row = response.rows[i];
                    _.forOwn(row, function (val, key) {
                        if (val instanceof Uuid) {
                            row[key] = val.toString();
                        } else if (val instanceof Long) {
                            row[key] = parseInt(val.toString(), 10);
                        }
                    });
                }
            }
            callback(null, (!_.isEmpty(response) && Array.isArray(response.rows) ? response.rows : []));
        });
    }

    /**
     * @function findOne
     * @description Finds a list of matching rows
     * @memberOf BaseModel
     * @access public
     * @param arg1
     * @param arg2
     * @param arg3
     * @return {(object)} Returns only one matching object
     */
    function findOne(arg1, arg2, arg3) {

        var filters = {},
            options = {},
            callback = null;
        // throw error if no argument was provided
        if (arguments.length === 0) {
            throw new Error('LIB_CAS_BM_9');
        }

        if (arguments.length === 1) {
            callback = arg1;
        } else if (arguments.length === 2) {
            filters = arg1;
            callback = arg2;
        } else {
            filters = arg1;
            options = arg2;
            callback = arg3;
        }

        if (!_.isPlainObject(filters)) {
            throw new Error('LIB_CAS_BM_10');
        }
        if (!_.isPlainObject(options)) {
            throw new Error('LIB_CAS_BM_11');
        }

        find(filters, options, function (err, response) {
            if (err) {
                callback(err);
                return;
            }
            callback(null, (response.length > 0 ? response[0] : null));
        });
    }

    /**
     * @function findMap
     * @description Finds a list of matching rows, then creates a dictionary based on keys
     * @memberOf BaseModel
     * @access public
     * @param arg1
     * @param arg2
     * @param arg3
     * @return {(object)} Returns only one matching object
     */
    function findMap(keys, arg2, arg3, arg4) {

        var filters = {},
            options = {},
            callback = null;
        // throw error if not enough arguments was provided
        if (arguments.length < 2) {
            throw new Error('LIB_CAS_BM_13');
        }

        if (arguments.length === 2) {
            callback = arg2;
        } else if (arguments.length === 3) {
            filters = arg2;
            callback = arg3;
        } else {
            filters = arg2;
            options = arg3;
            callback = arg4;
        }

        // argument validation
        if (Array.isArray(keys)) {
            if (keys.length === 0) {
                throw new Error('LIB_UTIL_2');
            }
        } else if (typeof keys !== 'string') {
            throw new Error('LIB_UTIL_2');
        }
        if (!_.isPlainObject(filters)) {
            throw new Error('LIB_CAS_BM_15');
        }
        if (!_.isPlainObject(options)) {
            throw new Error('LIB_CAS_BM_16');
        }

        find(filters, options, function (err, response) {
            if (err) {
                callback(err);
                return;
            }

            // Util.createMapFromCollection must be wrapped in try catch since it throws exception and
            // callback never gets called and falls into infinite loop
            try {
                var map = Util.createMapFromCollection(response, keys);
                callback(null, map);
            } catch (err) {
                callback(err);
            }
        });
    }

    /**
     * @function create
     * @description Creates a domain entry with the nameValueMap
     * @memberOf Domains
     * @param {(Object)} nameValueMap object to be inserted into database
     * @param {(function)} callback function to run after the query is executed
     * @return {String|Bool} Returns an error code and boolean result
     */
    function insert(insertMap, options, callback) {

        // throw error if no argument was provided
        if (arguments.length < 2) {
            throw new Error('LIB_CAS_BM_18');
        }

        if (arguments.length === 2) {
            callback = options;
            options = {};
        }

        if (!_.isPlainObject(insertMap) || _.isEmpty(insertMap)) {
            throw new Error('LIB_CAS_BM_19');
        }
        if (!_.isPlainObject(options)) {
            throw new Error('LIB_CAS_BM_20');
        }

        var query = new InsertQuery(keyspace, table, hostName);
        query.insertValues(insertMap);
        if (!_.isEmpty(options)) {
            query.usingOptions(options);
        }
        query.execute(callback);
    }

    /**
     * @function update
     * @description Updates a domain entry with the nameValueMap
     * @memberOf Domains
     * @param {(Object)} primaryKeyDict to filter for the primary keys
     * @param {(Object)} nameValueMap with keys and values to update to
     * @param {(function)} callback function to run after the query is executed
     * @return {String|Bool} Returns an error code and boolean result
     */
    function update(filters, updateMap, options, callback) {

        if (arguments.length < 3) {
            throw new Error('LIB_CAS_BM_22');
        }

        if (arguments.length === 3) {
            callback = options;
            options = {};
        }

        if (!_.isPlainObject(filters) || _.isEmpty(filters)) {
            throw new Error('LIB_CAS_BM_23');
        }
        if (!_.isPlainObject(updateMap) || _.isEmpty(updateMap)) {
            throw new Error('LIB_CAS_BM_24');
        }
        if (!_.isPlainObject(options)) {
            throw new Error('LIB_CAS_BM_25');
        }

        var query = new UpdateQuery(keyspace, table, writable, hostName);
        query.update().set(updateMap).where(filters);
        if (!_.isEmpty(options)) {
            query.usingOptions(options);
        }
        query.execute(callback);
    }

    /**
     * @function delete
     * @memberOf BaseModel
     * @description Deletes all matching elements
     * @param {(Object)} filters
     * @param {(Object)} options
     * @param {(function)} callback
     * @return {bool}
     * @public
     */
    function delete_(filters, options, callback) {
        if (arguments.length < 2) {
            throw new Error('LIB_CAS_BM_27');
        }

        if (arguments.length === 2) {
            callback = options;
            options = {};
        }

        if (!_.isPlainObject(filters) || _.isEmpty(filters)) {
            throw new Error('LIB_CAS_BM_28');
        }
        if (!_.isPlainObject(options)) {
            throw new Error('LIB_CAS_BM_29');
        }

        var query = new DeleteQuery(schemaMap.keyspace, schemaMap.table, hostName);
        query.delete().where(filters);

        if (!_.isEmpty(options)) {
            query.usingOptions(options);
        }
        query.execute(callback);
    }

    // problems with schema map need to be caught at the time of initialization
    validateSchemaMap();

    this.validateSchemaMap = validateSchemaMap;
    this.getSchemaMap = getSchemaMap;
    this.setKeyspace = setKeyspace;
    this.find = find;
    this.findOne = findOne;
    this.findMap = findMap;
    this.insert = insert;
    this.update = update;
    this.delete = delete_;
}

module.exports = BaseModel;