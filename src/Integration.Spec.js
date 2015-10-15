var rewire = require('rewire');
var _ = require('lodash');
var BaseModel = rewire('./BaseModel.js');
var ReadQuery = rewire('./ReadQuery.js');
var InsertQuery = rewire('./InsertQuery.js');
var UpdateQuery = rewire('./UpdateQuery.js');
var DeleteQuery = rewire('./DeleteQuery.js');
var Statement = rewire('./_Statement.js');
var DBClientMock = require('./DBClient.Spec.js');

describe('lib/cassandra Integration Tests', function () {
    var dbClientSpy, schemaMap, baseModel, schemaMapRef = {
        keyspace: 'keyspace',
        table: 'table'
    };
    beforeEach(function () {
        dbClientSpy = DBClientMock.createSpy();
        dbClientSpy.getConnection().execute.and.callFake(function (query, queryParams, prepare, callback) {
            callback(null, {});
        });
        DeleteQuery.__set__('_Statement', Statement);
        DeleteQuery.__set__('cassandraDB', dbClientSpy);
        InsertQuery.__set__('_Statement', Statement);
        InsertQuery.__set__('cassandraDB', dbClientSpy);
        UpdateQuery.__set__('_Statement', Statement);
        UpdateQuery.__set__('cassandraDB', dbClientSpy);
        ReadQuery.__set__('_Statement', Statement);
        ReadQuery.__set__('cassandraDB', dbClientSpy);
        BaseModel.__set__('DeleteQuery', DeleteQuery);
        BaseModel.__set__('InsertQuery', InsertQuery);
        BaseModel.__set__('UpdateQuery', UpdateQuery);
        BaseModel.__set__('ReadQuery', ReadQuery);

        schemaMap = _.cloneDeep(schemaMapRef);
        baseModel = new BaseModel(schemaMap);
    });

    describe('find(callback)', function () {
        it('should throw LIB_CAS_BM_14 if it is called without a callback', function (done) {
            expect(function(){
                baseModel.find();
            }).toThrow(new Error('LIB_CAS_BM_14'));
            done();
        });
        it('should execute with only callback', function (done) {
            baseModel.find(function () {
                var args = dbClientSpy.getConnection().execute.calls.mostRecent().args;
                expect(args[0]).toEqual('SELECT * FROM keyspace.table');
                expect(args[1]).toEqual([]);
                expect(args[2]).toEqual({prepare: true});
                done();
            });
        });
    });

    describe('find(filters, callback)', function () {

        it('should execute with empty filters and callback', function (done) {
            var filters = {};
            baseModel.find(filters, function(){
                var args = dbClientSpy.getConnection().execute.calls.mostRecent().args;
                expect(args[0]).toEqual('SELECT * FROM keyspace.table');
                expect(args[1]).toEqual([]);
                expect(args[2]).toEqual({ prepare: true });
                done();
            });
        });

        it('should execute with eq filters and callback', function (done) {
            var filters = {key1: 'val1'};
            baseModel.find(filters, function(){
                var args = dbClientSpy.getConnection().execute.calls.mostRecent().args;
                expect(args[0]).toEqual('SELECT * FROM keyspace.table WHERE key1 = ?');
                expect(args[1]).toEqual(['val1']);
                expect(args[2]).toEqual({ prepare: true });
                done();
            });
        });

        it('should execute with IN filters and callback', function (done) {
            var filters = {key1: ['val1', 'val2']};
            baseModel.find(filters, function(){
                var args = dbClientSpy.getConnection().execute.calls.mostRecent().args;
                expect(args[0]).toEqual('SELECT * FROM keyspace.table WHERE key1 IN (?, ?)');
                expect(args[1]).toEqual(['val1', 'val2']);
                expect(args[2]).toEqual({ prepare: true });
                done();
            });
        });

        it('should execute with range filters and callback', function (done) {
            var filters = {key1: {gte: 'val1'}};
            baseModel.find(filters, function(){
                var args = dbClientSpy.getConnection().execute.calls.mostRecent().args;
                expect(args[0]).toEqual('SELECT * FROM keyspace.table WHERE key1 >= ?');
                expect(args[1]).toEqual(['val1']);
                expect(args[2]).toEqual({ prepare: true });
                done();
            });
        });

        it('should throw LIB_CAS_BM_15 if filters is not a plain object', function (done) {
            var callback = function(){};
            expect(function(){
                baseModel.find([], callback);
            }).toThrow(new Error('LIB_CAS_BM_15'));
            expect(function(){
                baseModel.find('string', callback);
            }).toThrow(new Error('LIB_CAS_BM_15'));
            expect(function(){
                baseModel.find(new Date(), callback);
            }).toThrow(new Error('LIB_CAS_BM_15'));
            done();
        });
    });

    describe('find(filters, options, callback)', function () {

        it('should execute with empty filters, empty options and callback', function (done) {
            var filters = {};
            var options = {};
            baseModel.find(filters, options, function(){
                var args = dbClientSpy.getConnection().execute.calls.mostRecent().args;
                expect(args[0]).toEqual('SELECT * FROM keyspace.table');
                expect(args[1]).toEqual([]);
                expect(args[2]).toEqual({ prepare: true });
                done();
            });
        });
        it('should execute with limit', function (done) {
            var filters = {};
            var options = {limit: 100};
            baseModel.find(filters, options, function(){
                var args = dbClientSpy.getConnection().execute.calls.mostRecent().args;
                expect(args[0]).toEqual('SELECT * FROM keyspace.table LIMIT 100');
                expect(args[1]).toEqual([]);
                expect(args[2]).toEqual({ prepare: true });
                done();
            });
        });

        it('should execute with limit, sortBy, and allowFiltering', function (done) {
            var options = {limit: 1000, sortBy: 'key1', sortOrder: 'ASC', allowFiltering: true};
            baseModel.find({}, options, function(){
                var args = dbClientSpy.getConnection().execute.calls.mostRecent().args;
                expect(args[0]).toEqual('SELECT * FROM keyspace.table ORDER BY key1 ASC LIMIT 1000 ALLOW FILTERING');
                expect(args[1]).toEqual([]);
                expect(args[2]).toEqual({ prepare: true });
                done();
            });
        });

        it('should execute with columns', function (done) {
            var options = {columns: ['key1', 'key2']};
            baseModel.find({}, options, function(){
                var args = dbClientSpy.getConnection().execute.calls.mostRecent().args;
                expect(args[0]).toEqual('SELECT key1, key2 FROM keyspace.table');
                expect(args[1]).toEqual([]);
                expect(args[2]).toEqual({ prepare: true });
                done();
            });
        });

        it('should execute with readable', function (done) {
            schemaMap.readable = ['key1', 'key2'];
            baseModel = new BaseModel(schemaMap);
            baseModel.find({}, {}, function(){
                var args = dbClientSpy.getConnection().execute.calls.mostRecent().args;
                expect(args[0]).toEqual('SELECT key1, key2 FROM keyspace.table');
                expect(args[1]).toEqual([]);
                expect(args[2]).toEqual({ prepare: true });
                done();
            });
        });

        it('should throw LIB_CAS_RQ_5 if columns is an empty array', function (done) {
            var callback = function(){};
            var options = {columns:[]};
            expect(function(){
                baseModel.find({}, options, callback);
            }).toThrow(new Error('LIB_CAS_RQ_5'));
            done();
        });

        it('should throw LIB_CAS_RQ_3 if readable is an empty array', function (done) {
            var callback = function(){};
            schemaMap.readable = [];
            baseModel = new BaseModel(schemaMap);
            expect(function(){
                baseModel.find({}, {}, callback);
            }).toThrow(new Error('LIB_CAS_RQ_3'));
            done();
        });

        it('should throw LIB_CAS__ST_5 if unreadable key is requested', function (done) {
            var callback = function(){};
            var options = {columns:['key1', 'key3']};
            schemaMap.readable = ['key1', 'key2'];
            baseModel = new BaseModel(schemaMap);
            expect(function(){
                baseModel.find({}, options, callback);
            }).toThrow(new Error('LIB_CAS__ST_5'));
            done();
        });

        it('should throw LIB_CAS_BM_16 if options is not a plain object', function (done) {
            var callback = function(){};
            expect(function(){
                baseModel.find({}, [], callback);
            }).toThrow(new Error('LIB_CAS_BM_15'));
            expect(function(){
                baseModel.find({}, 'string', callback);
            }).toThrow(new Error('LIB_CAS_BM_15'));
            expect(function(){
                baseModel.find({}, new Date(), callback);
            }).toThrow(new Error('LIB_CAS_BM_15'));
            done();
        });

    });

    describe('insert(insertMap, callback)', function () {
        it('should throw LIB_CAS_BM_16 is called without filters or callback', function (done) {
            expect(function(){
                baseModel.insert(function(){});
            }).toThrow(new Error('LIB_CAS_BM_16'));
            expect(function(){
                baseModel.insert({key1: 'val1'});
            }).toThrow(new Error('LIB_CAS_BM_16'));
            done();
        });

        it('should execute with only insertMap and callback', function (done) {
            baseModel.insert({key1: 'val1'}, function () {
                var args = dbClientSpy.getConnection().execute.calls.mostRecent().args;
                expect(args[0]).toEqual('INSERT INTO keyspace.table (key1) VALUES (?)');
                expect(args[1]).toEqual(['val1']);
                expect(args[2]).toEqual({prepare: true});
                done();
            });
        });

        it('should insert multiple keys and non-string values', function (done) {
            var date = new Date();
            baseModel.insert({key1: 'val1', key2: date}, function () {
                var args = dbClientSpy.getConnection().execute.calls.mostRecent().args;
                expect(args[0]).toEqual('INSERT INTO keyspace.table (key1, key2) VALUES (?, ?)');
                expect(args[1]).toEqual(['val1', date]);
                expect(args[2]).toEqual({prepare: true});
                done();
            });
        });

        it('should throw LIB_CAS_BM_17 if insertMap is not a non-empty {}', function (done) {
            var callback = function(){};
            expect(function(){
                baseModel.insert([], callback);
            }).toThrow(new Error('LIB_CAS_BM_17'));
            expect(function(){
                baseModel.insert({}, callback);
            }).toThrow(new Error('LIB_CAS_BM_17'));
            expect(function(){
                baseModel.insert(new Date(), callback);
            }).toThrow(new Error('LIB_CAS_BM_17'));
            done();
        });

    });

    describe('insert(insertMap, options, callback)', function () {
        it('should insert without options', function (done) {
            var date = new Date();
            baseModel.insert({key1: 'val1', key2: date}, {}, function () {
                var args = dbClientSpy.getConnection().execute.calls.mostRecent().args;
                expect(args[0]).toEqual('INSERT INTO keyspace.table (key1, key2) VALUES (?, ?)');
                expect(args[1]).toEqual(['val1', date]);
                expect(args[2]).toEqual({prepare: true});
                done();
            });
        });

        it('should insert with TTL and timestamp options', function (done) {
            var options = {TTL: 1000, timestamp: 2000};
            baseModel.insert({key1: 'val1', key2: 'val2'}, options, function () {
                var args = dbClientSpy.getConnection().execute.calls.mostRecent().args;
                expect(args[0]).toEqual('INSERT INTO keyspace.table (key1, key2) VALUES (?, ?) USING TTL 1000 AND TIMESTAMP 2000');
                expect(args[1]).toEqual(['val1', 'val2']);
                expect(args[2]).toEqual({prepare: true});
                done();
            });
        });

        it('should throw LIB_CAS_BM_18 if insertMap is not a non-empty {}', function (done) {
            var callback = function(){};
            expect(function(){
                baseModel.insert({key1: 'val1'}, [], callback);
            }).toThrow(new Error('LIB_CAS_BM_18'));
            expect(function(){
                baseModel.insert({key1: 'val1'}, 'string', callback);
            }).toThrow(new Error('LIB_CAS_BM_18'));
            expect(function(){
                baseModel.insert({key1: 'val1'}, new Date(),callback);
            }).toThrow(new Error('LIB_CAS_BM_18'));
            done();
        });
    });

    describe('update(filters, updateMap, callback)', function(){
        it('should throw LIB_CAS_BM_19 if filters, updateMap, or callback is missing', function (done) {
            var callback = function(){};
            expect(function(){
                baseModel.update({}, {key1: 'val1'});
            }).toThrow(new Error('LIB_CAS_BM_19'));
            expect(function(){
                baseModel.update(callback);
            }).toThrow(new Error('LIB_CAS_BM_19'));
            done();
        });

        it('should update without options', function (done) {
            var filters = {key1: 'val1'};
            var updateMap = {key1: 'val1'};
            baseModel.update(filters, updateMap, function () {
                var args = dbClientSpy.getConnection().execute.calls.mostRecent().args;
                expect(args[0]).toEqual('UPDATE keyspace.table SET key1 = ? WHERE key1 = ?');
                expect(args[1]).toEqual(['val1', 'val1']);
                expect(args[2]).toEqual({prepare: true});
                done();
            });
        });

        it('should update with filters', function (done) {
            var filters = {key1: 'val1'};
            var updateMap = {key2: 'val2'};
            baseModel.update(filters, updateMap, function () {
                var args = dbClientSpy.getConnection().execute.calls.mostRecent().args;
                expect(args[0]).toEqual('UPDATE keyspace.table SET key2 = ? WHERE key1 = ?');
                expect(args[1]).toEqual(['val2', 'val1']);
                expect(args[2]).toEqual({prepare: true});
                done();
            });
        });

        it('should update multiple columns and non-string columns', function (done) {
            var filters = {key1: 'val1'};
            var date = new Date();
            var updateMap = {key1: 'val1', key2: date};
            baseModel.update(filters, updateMap, function () {
                var args = dbClientSpy.getConnection().execute.calls.mostRecent().args;
                expect(args[0]).toEqual('UPDATE keyspace.table SET key1 = ?, key2 = ? WHERE key1 = ?');
                expect(args[1]).toEqual(['val1', date, 'val1']);
                expect(args[2]).toEqual({prepare: true});
                done();
            });
        });

        it('should throw LIB_CAS_BM_20 if filters is not a non-empty object', function (done) {
            var callback = function(){};
            expect(function(){
                baseModel.update([], {key1: 'val1'}, callback);
            }).toThrow(new Error('LIB_CAS_BM_20'));
            expect(function(){
                baseModel.update('string', {key1: 'val1'}, callback);
            }).toThrow(new Error('LIB_CAS_BM_20'));
            expect(function(){
                baseModel.update(new Date(), {key1: 'val1'}, callback);
            }).toThrow(new Error('LIB_CAS_BM_20'));
            expect(function(){
                baseModel.update({}, {key1: 'val1'}, callback);
            }).toThrow(new Error('LIB_CAS_BM_20'));
            done();
        });

        it('should throw LIB_CAS_BM_21 if updateMap is not a non-empty object', function (done) {
            var filters = {key1: 'val1'};
            var callback = function(){};
            expect(function(){
                baseModel.update(filters, [], callback);
            }).toThrow(new Error('LIB_CAS_BM_21'));
            expect(function(){
                baseModel.update(filters, 'string', callback);
            }).toThrow(new Error('LIB_CAS_BM_21'));
            expect(function(){
                baseModel.update(filters, new Date(), callback);
            }).toThrow(new Error('LIB_CAS_BM_21'));
            expect(function(){
                baseModel.update(filters, {}, callback);
            }).toThrow(new Error('LIB_CAS_BM_21'));
            done();
        });
    });

    describe('update(filters, updateMap, options, callback)', function(){
        it('should update with options', function (done) {
            var filters = {key1: 'val1'};
            var updateMap = {key1: 'val1'};
            var options = {TTL: 100};
            baseModel.update(filters, updateMap, options, function () {
                var args = dbClientSpy.getConnection().execute.calls.mostRecent().args;
                expect(args[0]).toEqual('UPDATE keyspace.table USING TTL 100 SET key1 = ? WHERE key1 = ?');
                expect(args[1]).toEqual(['val1', 'val1']);
                expect(args[2]).toEqual({prepare: true});
                done();
            });
        });

        it('should throw LIB_CAS_BM_22 if updateMap is not an object', function (done) {
            var filters = {key1: 'val1'};
            var callback = function(){};
            expect(function(){
                baseModel.update(filters, {key1: 'key2'}, [], callback);
            }).toThrow(new Error('LIB_CAS_BM_22'));
             expect(function(){
                baseModel.update(filters, {key1: 'key2'}, 'string', callback);
            }).toThrow(new Error('LIB_CAS_BM_22'));
             expect(function(){
                baseModel.update(filters, {key1: 'key2'}, new Date(), callback);
            }).toThrow(new Error('LIB_CAS_BM_22'));
            done();
        });
    });

    describe('delete(filters, callback)', function(){
        it('should throw LIB_CAS_BM_23 if filters or callback is missing', function (done) {
            var callback = function(){};
            expect(function(){
                baseModel.delete();
            }).toThrow(new Error('LIB_CAS_BM_23'));
            expect(function(){
                baseModel.delete(callback);
            }).toThrow(new Error('LIB_CAS_BM_23'));
            done();
        });

        it('should delete without options', function (done) {
            var filters = {key1: 'val1'};
            baseModel.delete(filters, function () {
                var args = dbClientSpy.getConnection().execute.calls.mostRecent().args;
                expect(args[0]).toEqual('DELETE FROM keyspace.table WHERE key1 = ?');
                expect(args[1]).toEqual(['val1']);
                expect(args[2]).toEqual({prepare: true});
                done();
            });
        });

        it('should throw LIB_CAS_BM_24 if filters is not a non-empty object', function (done) {

            var callback = function(){};
            expect(function(){
                baseModel.delete({}, callback);
            }).toThrow(new Error('LIB_CAS_BM_24'));
            expect(function(){
                baseModel.delete([], callback);
            }).toThrow(new Error('LIB_CAS_BM_24'));
            expect(function(){
                baseModel.delete('string', callback);
            }).toThrow(new Error('LIB_CAS_BM_24'));
            expect(function(){
                baseModel.delete(new Date(), callback);
            }).toThrow(new Error('LIB_CAS_BM_24'));
            done();
        });
    });

    describe('delete(filters, options, callback)', function(){
        it('should delete with options', function (done) {
            var filters = {key1: 'val1'};
            var options = {timestamp: 2000};
            baseModel.delete(filters, options, function () {
                var args = dbClientSpy.getConnection().execute.calls.mostRecent().args;
                expect(args[0]).toEqual('DELETE FROM keyspace.table USING TIMESTAMP 2000 WHERE key1 = ?');
                expect(args[1]).toEqual(['val1']);
                expect(args[2]).toEqual({prepare: true});
                done();
            });
        });

        it('should throw LIB_CAS_BM_25 if filters is not an object', function (done) {
            var filters = {key1: 'key2'};
            var callback = function(){};
            expect(function(){
                baseModel.delete(filters, [], callback);
            }).toThrow(new Error('LIB_CAS_BM_25'));
            expect(function(){
                baseModel.delete(filters, 'string', callback);
            }).toThrow(new Error('LIB_CAS_BM_25'));
            expect(function(){
                baseModel.delete(filters, new Date(), callback);
            }).toThrow(new Error('LIB_CAS_BM_25'));
            done();
        });
    });


});