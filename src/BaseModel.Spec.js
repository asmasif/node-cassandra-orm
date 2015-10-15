var rewire = require('rewire');
var _ = require('lodash');
var stringify = require('json-stable-stringify');
var BaseModel = rewire('./BaseModel.js');
var ReadQueryMock = require('./ReadQuery.Spec.js');
var InsertQueryMock = require('./InsertQuery.Spec.js');
var UpdateQueryMock = require('./UpdateQuery.Spec.js');
var DeleteQueryMock = require('./DeleteQuery.Spec.js');

describe('lib/cassandra/BaseModel.js', function () {
    var schemaMap, baseModel, schemaMapRef = {
        keyspace: 'keyspace',
        table: 'table',
        fields: {
            key1: {
                type: 'string',
                required: true
            },
            key2: {
                type: 'string',
                required: true
            },
            key3: {
                type: 'int',
                required: true
            },
            key4: {
                type: 'string',
                required: true
            }
        },
        primaryKeys: ['key1'],
        primaryIndices: ['key1'],
        secondaryIndices: ['key2', 'key3'],
        readRestrictions: [],
        updateRestrictions: ['key1']
    };
    beforeEach(function () {
        schemaMap = _.cloneDeep(schemaMapRef);
    });
    describe('validateSchemaMap()', function () {
        it('should throw LIB_CAS_BM_1 if schemaMap.keyspace is not valid', function () {
            schemaMap.keyspace = undefined;
            expect(function () {
                baseModel = new BaseModel(schemaMap);
                baseModel.getSchemaMap();
            }).toThrow(new Error('LIB_CAS_BM_1'));
            schemaMap.keyspace = {};
            expect(function () {
                baseModel = new BaseModel(schemaMap);
                baseModel.getSchemaMap();
            }).toThrow(new Error('LIB_CAS_BM_1'));
        });
        it('should throw LIB_CAS_BM_2 if schemaMap.table is not valid', function () {
            schemaMap.table = undefined;
            expect(function () {
                baseModel = new BaseModel(schemaMap);
                baseModel.getSchemaMap();
            }).toThrow(new Error('LIB_CAS_BM_2'));
            schemaMap.table = {};
            expect(function () {
                baseModel = new BaseModel(schemaMap);
                baseModel.getSchemaMap();
            }).toThrow(new Error('LIB_CAS_BM_2'));
        });
    });

    describe('getSchemaMap ()', function () {
        beforeEach(function () {
            baseModel = new BaseModel(schemaMap);
        });
        it('should return a copy of the schemaMap the base model was created with', function () {
            var passedSchemaMap = baseModel.getSchemaMap();
            expect(stringify(schemaMap)).toEqual(stringify(passedSchemaMap));
            schemaMap.keyspace = 'test_keyspace';
            expect(schemaMap.keyspace).not.toEqual(passedSchemaMap.keyspace);
        });
        it('should return a reference to the same schemaMap that was passed to base model', function () {
            var passedSchemaMap = baseModel.getSchemaMap(true);
            schemaMap.keyspace = 'new_keyspace';
            expect(schemaMap.keyspace).toEqual(passedSchemaMap.keyspace);
        });
    });


    describe('setKeyspace ()', function () {
        it('should set keyspace for the base model', function () {
            baseModel = new BaseModel(schemaMap);
            var oldSchemaMap = baseModel.getSchemaMap();
            baseModel.setKeyspace('sample_keyspace');
            var newSchemaMap = baseModel.getSchemaMap();
            expect(oldSchemaMap.keyspace).not.toEqual(newSchemaMap.keyspace);
            expect(newSchemaMap.keyspace).toEqual('sample_keyspace');
        });
    });

    describe('find (arg1, arg2, arg3)', function () {
        var ReadQuery;
        beforeEach(function () {
            ReadQuery = ReadQueryMock.createSpy();
            BaseModel.__set__('ReadQuery', ReadQuery);
            baseModel = new BaseModel(schemaMap);
        });
        it('should throw LIB_CAS_BM_14 if no argument was provided', function () {
            expect(function () {
                baseModel.find();
            }).toThrow(new Error('LIB_CAS_BM_14'));
        });

        it('should throw LIB_CAS_BM_15 if filters is not an object or empty string', function () {
            var filters = 'NOT AN OBJECT OR EMPTY STRING';
            expect(function () {
                baseModel.find(filters, function () {
                });
            }).toThrow(new Error('LIB_CAS_BM_15'));
        });

        it('should execute query without errors when only callback is passed in', function () {
            var callback = function () {
            };
            baseModel.find(callback);
            expect(ReadQueryMock.spy.execute).toHaveBeenCalled();
        });

        it('should execute query without errors when valid filters, options, and callback are passed in', function () {
            var filters = {
                    key1: 'test'
                },
                options = {
                    limit: 1,
                    sortBy: 1,
                    allowFiltering: true,
                    postProcess: true
                },
                callback = jasmine.createSpy();

            baseModel.find(filters, options, callback);
            ReadQueryMock.spy.execute.calls.mostRecent().args[0](null, {
                rows: [{
                    key1: 1,
                    key2: '2'
                }]
            });

            expect(callback).toHaveBeenCalled();
        });
        it('should call callback with error if executing query is unsuccessful', function () {
            var filters = {
                    key1: 'test'
                },
                options = {
                    limit: 1,
                    sortBy: 1,
                    allowFiltering: true,
                    postProcess: true
                },
                callback = jasmine.createSpy();

            baseModel.find(filters, options, callback);
            ReadQueryMock.spy.execute.calls.mostRecent().args[0]('error', {
                rows: [{
                    key1: 1,
                    key2: '2'
                }]
            });

            expect(callback).toHaveBeenCalledWith('error');
        });
    });
    describe('findOne (arg1, arg2, arg3)', function () {
        var ReadQuery;
        beforeEach(function () {
            ReadQuery = ReadQueryMock.createSpy();
            BaseModel.__set__('ReadQuery', ReadQuery);
            baseModel = new BaseModel(schemaMap);
        });

        it('should throw LIB_CAS_BM_14 error if no arguments are passed in', function () {
            expect(function () {
                baseModel.findOne();
            }).toThrowError('LIB_CAS_BM_14');
        });

        it('should execute query without errors when only callback is passed in', function () {
            var callback = function () {
            };
            baseModel.findOne(callback);
            expect(ReadQueryMock.spy.execute).toHaveBeenCalled();
        });

        it('should return to callback one valid response when valid filters and callback are passed in', function () {
            var filters = {
                    key1: 'test'
                },
                callback = jasmine.createSpy(),
                resArr = [{
                    key1: 1,
                    key2: '2'
                }];

            baseModel.findOne(filters, callback);
            ReadQueryMock.spy.execute.calls.mostRecent().args[0](null, {
                rows: resArr
            });

            expect(callback).toHaveBeenCalledWith(null, resArr[0]);
        });

        it('should execute query without errors when valid filters, options, and callback are passed in', function () {
            var filters = {
                    key1: 'test'
                },
                options = {
                    limit: 1,
                    sortBy: 1,
                    allowFiltering: true,
                    postProcess: true
                },
                callback = jasmine.createSpy();

            baseModel.findOne(filters, options, callback);
            ReadQueryMock.spy.execute.calls.mostRecent().args[0](null, {
                rows: [{
                    key1: 1,
                    key2: '2'
                }]
            });

            expect(callback).toHaveBeenCalled();
        });
        it('should call callback with error if executing query is unsuccessful', function () {
            var filters = {
                    key1: 'test'
                },
                options = {
                    limit: 1,
                    sortBy: 1,
                    allowFiltering: true,
                    postProcess: true
                },
                callback = jasmine.createSpy();

            baseModel.findOne(filters, options, callback);
            ReadQueryMock.spy.execute.calls.mostRecent().args[0]('error', {
                rows: [{
                    key1: 1,
                    key2: '2'
                }]
            });

            expect(callback).toHaveBeenCalledWith('error');
        });


    });
    describe('findMap (arg1, arg2, arg3)', function () {
        var ReadQuery,
            util;
        beforeEach(function () {
            ReadQuery = ReadQueryMock.createSpy();
            util = jasmine.createSpyObj('util', ['createMapFromCollection']);
            BaseModel.__set__('ReadQuery', ReadQuery);
            BaseModel.__set__('util', util);
            baseModel = new BaseModel(schemaMap);
        });

        it('should throw LIB_CAS_BM_14 error if no arguments are passed in', function () {
            expect(function () {
                baseModel.findMap();
            }).toThrowError('LIB_CAS_BM_14');
        });

        it('should throw LIB_CAS_BM_14 error if only one argument is passed in', function () {

            var callback = function () {
            };
            expect(function () {
                baseModel.findMap(callback);
            }).toThrowError('LIB_CAS_BM_14');
        });

        it('should return to callback a map when valid keys and callback are passed in', function () {
            var keys = ['key1'],
                callback = jasmine.createSpy(),
                resArr = [{
                    key1: 1,
                    key2: '2'
                }],
                pkeyMap = {};

            util.createMapFromCollection.and.callFake(function (response, keys) {
                response.forEach(function (row, idx) {
                    pkeyMap[keys[idx]] = row;
                });
                return pkeyMap;
            });

            baseModel.findMap(keys, callback);
            ReadQueryMock.spy.execute.calls.mostRecent().args[0](null, {
                rows: resArr
            });

            expect(callback).toHaveBeenCalledWith(null, pkeyMap);
        });


        it('should return to callback a map when valid keys, filters and callback are passed in', function () {
            var keys = ['key1'],
                filters = {
                    key1: 'test'
                },
                callback = jasmine.createSpy(),
                resArr = [{
                    key1: 1,
                    key2: '2'
                }],
                pkeyMap = {};

            util.createMapFromCollection.and.callFake(function (response, keys) {
                response.forEach(function (row, idx) {
                    pkeyMap[keys[idx]] = row;
                });
                return pkeyMap;
            });

            baseModel.findMap(keys, filters, callback);
            ReadQueryMock.spy.execute.calls.mostRecent().args[0](null, {
                rows: resArr
            });

            expect(callback).toHaveBeenCalledWith(null, pkeyMap);
        });

        it('should execute query without errors when valid keys, filters, options, and callback are passed in', function () {
            var keys= ['key1'],
                filters = {
                    key1: 'test'
                },
                options = {
                    limit: 1,
                    sortBy: 1,
                    allowFiltering: true,
                    postProcess: true
                },
                callback = jasmine.createSpy(),
                resArr = [{
                    key1: 1,
                    key2: '2'
                }],
                pkeyMap = {};

            util.createMapFromCollection.and.callFake(function (response, keys) {
                response.forEach(function (row, idx) {
                    pkeyMap[keys[idx]] = row;
                });
                return pkeyMap;
            });

            baseModel.findMap(keys, filters, options, callback);
            ReadQueryMock.spy.execute.calls.mostRecent().args[0](null, {
                rows: resArr
            });

            expect(callback).toHaveBeenCalledWith(null, pkeyMap);
        });
        it('should call callback with error if executing query is unsuccessful', function () {
            var filters = {
                    key1: 'test'
                },
                options = {
                    limit: 1,
                    sortBy: 1,
                    allowFiltering: true,
                    postProcess: true
                },
                callback = jasmine.createSpy();

            baseModel.findMap(filters, options, callback);
            ReadQueryMock.spy.execute.calls.mostRecent().args[0]('error', {
                rows: [{
                    key1: 1,
                    key2: '2'
                }]
            });

            expect(callback).toHaveBeenCalledWith('error');
        });


    });
    describe('insert (insertMap, options, callback)', function () {
        var spy;
        beforeEach(function () {
            spy = InsertQueryMock.createSpy();
            BaseModel.__set__('InsertQuery', spy);
            baseModel = new BaseModel(schemaMap);
        });
        it('should return an error if less than 2 arguments were provided', function (done) {
            var insertMap = {};
            expect(function () {
                baseModel.insert(insertMap);
            }).toThrow(new Error('LIB_CAS_BM_16'));
            done();
        });

        it('should return an error if insertMap is not an object or if it is an empty dict', function (done) {
            var insertMap = 'NOT AN OBJECT';
            expect(function () {
                baseModel.insert(insertMap, function () {
                });
            }).toThrow(new Error('LIB_CAS_BM_17'));
            insertMap = {};
            expect(function () {
                baseModel.insert(insertMap, function () {
                });
            }).toThrow(new Error('LIB_CAS_BM_17'));
            done();
        });
        it('should return an error if options is not an object', function (done) {
            var insertMap = {
                    test: 'test'
                },
                options = 'NOT AN OBJECT',
                callback = function () {
                };
            expect(function () {
                baseModel.insert(insertMap, options, callback);
            }).toThrow(new Error('LIB_CAS_BM_18'));
            done();
        });

        it('should execute the query if no error is thrown', function () {
            var insertMap = {
                    test: 'test'
                },
                options = {
                    test: 'test'
                },
                callback = function () {
                };

            baseModel.insert(insertMap, options, callback);
            expect(InsertQueryMock.spy.insertValues).toHaveBeenCalled();
            expect(InsertQueryMock.spy.execute).toHaveBeenCalledWith(callback);

        });
    });
    describe('update (filters, updateMap, options, callback)', function () {
        var spy;
        beforeEach(function () {
            spy = UpdateQueryMock.createSpy();
            BaseModel.__set__('UpdateQuery', spy);
            baseModel = new BaseModel(schemaMap);
        });
        it('should return an error if less than 3 arguments were provided', function (done) {
            var filters = {};
            expect(function () {
                baseModel.update(filters);
            }).toThrow(new Error('LIB_CAS_BM_19'));
            done();
        });
        it('should return an error if filters is not an object or if it is an empty dict', function (done) {
            var filters = 'NOT AN OBJECT',
                updateMap = {
                    test: 'test'
                },
                callback = function () {
                };
            expect(function () {
                baseModel.update(filters, updateMap, callback);
            }).toThrow(new Error('LIB_CAS_BM_20'));
            filters = {};
            expect(function () {
                baseModel.update(filters, updateMap, callback);
            }).toThrow(new Error('LIB_CAS_BM_20'));
            done();
        });
        it('should return an error if updateMap is not an object or if it is an empty dict', function (done) {
            var filters = {
                    test: 'test'
                },
                updateMap = 'NOT AN OBJECT',
                callback = function () {
                };
            expect(function () {
                baseModel.update(filters, updateMap, callback);
            }).toThrow(new Error('LIB_CAS_BM_21'));
            updateMap = {};
            expect(function () {
                baseModel.update(filters, updateMap, callback);
            }).toThrow(new Error('LIB_CAS_BM_21'));
            done();
        });
        it('should return an error if options is not an object', function (done) {
            var filters = {
                    test: 'test'
                },
                updateMap = {
                    test: 'test'
                },
                options = 'NOT AN OBJECT',
                callback = function () {
                };
            expect(function () {
                baseModel.update(filters, updateMap, options, callback);
            }).toThrow(new Error('LIB_CAS_BM_22'));
            done();
        });
        
        it('should execute update query when no error is thrown', function () {
            var filters = {
                    key1: [],
                    key2: {
                        test: ''
                    }
                },
                updateMap = {
                    test: 'test'
                },
                options = {
                    test: 'test'
                },
                callback = function () {
                };

            var customSchemaMap = {
                keyspace: 'keyspace',
                table: 'table',
                fields: {
                    key1: {
                        type: 'string',
                        required: true
                    },
                    key2: {
                        type: 'string',
                        required: true
                    },
                    key3: {
                        type: 'int',
                        required: true
                    },
                    key4: {
                        type: 'string',
                        required: true
                    }
                },
                primaryKeys: ['key1', 'key2'],
                primaryIndices: ['key1'],
                secondaryIndices: ['key2'],
                readRestrictions: [],
                updateRestrictions: ['key1']
            };

            baseModel = new BaseModel(customSchemaMap);
            baseModel.update(filters, updateMap, options, callback);
            expect(UpdateQueryMock.spy.execute).toHaveBeenCalledWith(callback);
        });
    });
    describe('delete_ (filters, options, callback)', function () {
        var spy;
        beforeEach(function () {
            spy = DeleteQueryMock.createSpy();
            BaseModel.__set__('DeleteQuery', spy);
            baseModel = new BaseModel(schemaMap);
        });
        it('should return an error if less than 2 arguments were provided', function (done) {
            var filters = {};
            expect(function () {
                baseModel.delete(filters);
            }).toThrow(new Error('LIB_CAS_BM_23'));
            done();
        });
        it('should return an error if filters is not an object or if it is an empty dict', function (done) {
            var filters = 'NOT AN OBJECT',
                callback = function () {
                };
            expect(function () {
                baseModel.delete(filters, callback);
            }).toThrow(new Error('LIB_CAS_BM_24'));
            filters = {};
            expect(function () {
                baseModel.delete(filters, callback);
            }).toThrow(new Error('LIB_CAS_BM_24'));
            done();
        });
        it('should return an error if options is not an object', function (done) {
            var filters = {
                    test: 'test'
                },
                options = 'NOT AN OBJECT',
                callback = function () {
                };
            expect(function () {
                baseModel.delete(filters, options, callback);
            }).toThrow(new Error('LIB_CAS_BM_25'));
            done();
        });
        it('should execute delete query if no errors are thrown', function () {
            var filters = {
                    key1: 'test'
                },
                options = {
                    test: 'test'
                },
                callback = function () {
                };

            baseModel.delete(filters, options, callback);
            expect(DeleteQueryMock.spy.execute).toHaveBeenCalledWith(callback);
        });
    });
});