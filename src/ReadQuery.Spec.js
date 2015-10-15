var rewire = require('rewire');
var ReadQuery = rewire('./ReadQuery.js');
var StatementMock = require('./_Statement.Spec.js'),
    DBClientMock = require('./DBClient.Spec.js');

describe('lib/cassandra/ReadQuery.js', function () {
    var query, spy, DBClientSpy;
    beforeEach(function () {
        DBClientSpy = DBClientMock.createSpy();
        spy = StatementMock.createSpy();
        ReadQuery.__set__('_Statement', spy);
        ReadQuery.__set__('cassandraDB', DBClientSpy);
        query = new ReadQuery('keyspace', 'table');
    });
    describe('toString ()', function () {
        it('should generate a blank query', function () {
            expect(query.toString()).toEqual('');
        });
        it('should return a query string with SELECT and WHERE', function () {
            expect(query.select().where({key1: 'val1'}).toString()).toEqual('SELECT WHERE');
        });
        it('should return a query string with SELECT WHERE LIMIT ORDER BY', function () {
            expect(query.select().where({key1: 'val1'}).limit(5).allowFiltering().orderBy('key1', 'ASC').toString())
                .toEqual('SELECT WHERE ORDER LIMIT ALLOW FILTERING');
        });
    });

    describe('execute (callback)', function () {
        it('should execute the query', function () {
            query.select().where({key1: 'val1'}).limit(5).allowFiltering().orderBy('key1', 'ASC').execute(function () {
            });
            expect(DBClientSpy.getConnection().execute.calls.argsFor(0)[0]).toEqual('SELECT WHERE ORDER LIMIT ALLOW FILTERING');
            expect(DBClientSpy.getConnection().execute.calls.argsFor(0)[1]).toEqual([]);
            expect(DBClientSpy.getConnection().execute.calls.argsFor(0)[2]).toEqual({prepare: true});
            expect(DBClientSpy.getConnection().execute.calls.argsFor(0)[3]).toEqual(jasmine.any(Function));
        });
    });
});

// Mock for testing purposes
var ReadQueryMock = function () {
    var self = this;
    this.createSpy = function () {
        return function () {
            self.spy = jasmine.createSpyObj('ReadQuery', ['select', 'where', 'orderBy', 'limit',
                'allowFiltering', 'toString', 'execute']);

            self.spy.select.and.returnValue(self.spy);
            self.spy.where.and.returnValue(self.spy);
            self.spy.orderBy.and.returnValue(self.spy);
            self.spy.limit.and.returnValue(self.spy);
            self.spy.allowFiltering.and.returnValue(self.spy);
            self.spy.toString.and.returnValue(self.spy);
            self.spy.execute.and.returnValue(self.spy);
            return self.spy;
        };
    };
};

module.exports = new ReadQueryMock();