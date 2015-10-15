var rewire = require('rewire');
var DeleteQuery = rewire('./DeleteQuery.js');
var StatementMock = require('./_Statement.Spec.js'),
    DBClientMock = require('./DBClient.Spec.js');

describe('lib/cassandra/DeleteQuery.js', function () {
    var query, spy, DBClientSpy;
    beforeEach(function () {
        DBClientSpy = DBClientMock.createSpy();

        spy = StatementMock.createSpy();
        DeleteQuery.__set__('_Statement', spy);
        DeleteQuery.__set__('cassandraDB', DBClientSpy);
        query = new DeleteQuery('keyspace', 'table');
    });
    describe('toString ()', function () {
        it('should generate a blank query', function () {
            expect(query.toString()).toEqual('');
        });
        it('should return a query string with DELETE and WHERE', function () {
            expect(query.delete().where({key1: 'val1'}).toString())
                .toEqual('DELETE WHERE');
        });
        it('should return a query string with DELETE, USING options and WHERE', function () {
            expect(query.delete().usingOptions({TTL: 1}).where({key1: 'val1'}).toString())
                .toEqual('DELETE USING WHERE');
        });
    });

    describe('execute (callback)', function () {
        it('should execute the query', function () {
            query.delete().where({key1: 'val1'}).execute(function () {
            });
            expect(DBClientSpy.getConnection().execute.calls.argsFor(0)[0]).toEqual('DELETE WHERE');
            expect(DBClientSpy.getConnection().execute.calls.argsFor(0)[1]).toEqual([]);
            expect(DBClientSpy.getConnection().execute.calls.argsFor(0)[2]).toEqual({prepare: true});
            expect(DBClientSpy.getConnection().execute.calls.argsFor(0)[3]).toEqual(jasmine.any(Function));
        });
    });
});

// Mock for testing purposes
var DeleteQueryMock = function () {
    var self = this;
    this.createSpy = function () {
        return function () {
            self.spy = jasmine.createSpyObj('DeleteQuery', ['delete', 'usingOptions', 'where', 'toString', 'execute']);
            self.spy.delete.and.returnValue(self.spy);
            self.spy.usingOptions.and.returnValue(self.spy);
            self.spy.where.and.returnValue(self.spy);
            self.spy.toString.and.returnValue(self.spy);

            return self.spy;
        };

    };
};

module.exports = new DeleteQueryMock();