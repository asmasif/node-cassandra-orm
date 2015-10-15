var rewire = require('rewire');
var UpdateQuery = rewire('./UpdateQuery.js');
var StatementMock = require('./_Statement.Spec.js'),
    DBClientMock = require('./DBClient.Spec.js');

describe('lib/cassandra/UpdateQuery.js', function () {
    var query, spy, DBClientSpy;

    beforeEach(function () {
        DBClientSpy = DBClientMock.createSpy();
        spy = StatementMock.createSpy();
        UpdateQuery.__set__('_Statement', spy);
        UpdateQuery.__set__('cassandraDB', DBClientSpy);
        query = new UpdateQuery('keyspace', 'table');
    });

    describe('toString ()', function () {
        it('should generate a blank query', function () {
            expect(query.toString()).toEqual('');
        });
        it('should return a query string with UPDATE, SET and WHERE', function () {
            expect(query.update().set({key1: 'val1'}).where({'key2': 'val2'}).toString()).toEqual('UPDATE SET WHERE');
        });
        it('should return a query string with UPDATE, USING, SET and WHERE', function () {
            expect(query.update().set({key1: 'val1'}).where({'key2': 'val2'}).usingOptions({TTL: 1}).toString()).toEqual('UPDATE USING SET WHERE');
        });
    });

    describe('execute (callback)', function () {
        it('should execute the query', function () {
            query.update().set({key1: 'val1'}).where({'key2': 'val2'}).usingOptions({TTL: 1}).execute(function () {
            });
            expect(DBClientSpy.getConnection().execute.calls.argsFor(0)[0]).toEqual('UPDATE USING SET WHERE');
            expect(DBClientSpy.getConnection().execute.calls.argsFor(0)[1]).toEqual([]);
            expect(DBClientSpy.getConnection().execute.calls.argsFor(0)[2]).toEqual({prepare: true});
            expect(DBClientSpy.getConnection().execute.calls.argsFor(0)[3]).toEqual(jasmine.any(Function));
        });
    });
});

// Mock for testing purposes
var UpdateQueryMock = function () {
    var self = this;
    this.createSpy = function () {
        return function () {
            self.spy = jasmine.createSpyObj('UpdateQuery', ['where', 'usingOptions', 'update', 'set', 'toString', 'execute']);

            self.spy.update.and.returnValue(self.spy);
            self.spy.set.and.returnValue(self.spy);
            self.spy.where.and.returnValue(self.spy);
            self.spy.usingOptions.and.returnValue(self.spy);
            self.spy.toString.and.returnValue(self.spy);

            return self.spy;
        };

    };
};

module.exports = new UpdateQueryMock();