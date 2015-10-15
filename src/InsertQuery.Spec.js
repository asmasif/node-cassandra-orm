var rewire = require('rewire');
var InsertQuery = rewire('./InsertQuery.js');
var StatementMock = require('./_Statement.Spec.js'),
    DBClientMock = require('./DBClient.Spec.js');

describe('lib/cassandra/InsertQuery.js', function () {
    var query, spy, DBClientSpy;
    beforeEach(function () {
        spy = StatementMock.createSpy();
        DBClientSpy = DBClientMock.createSpy();
        InsertQuery.__set__('_Statement', spy);
        InsertQuery.__set__('cassandraDB', DBClientSpy);
        query = new InsertQuery('keyspace', 'table');
    });

    describe('toString ()', function () {
        it('should generate a blank query', function () {
            expect(query.toString()).toEqual('');
        });
        it('should return a query string with INSERT', function () {
            expect(query.insertValues({key1: 'val1'}).toString()).toEqual('INSERT');
        });
        it('should return a query string with INSERT USING', function () {
            expect(query.insertValues({key1: 'val1'}).usingOptions({TTL: 1}).toString()).toEqual('INSERT USING');
        });
    });

    describe('execute (callback)', function () {
        it('should execute the query', function () {
            query.insertValues({key1: 'val1'}).usingOptions({TTL: 1}).execute(function () {
            });
            expect(DBClientSpy.getConnection().execute.calls.argsFor(0)[0]).toEqual('INSERT USING');
            expect(DBClientSpy.getConnection().execute.calls.argsFor(0)[1]).toEqual([]);
            expect(DBClientSpy.getConnection().execute.calls.argsFor(0)[2]).toEqual({prepare: true});
            expect(DBClientSpy.getConnection().execute.calls.argsFor(0)[3]).toEqual(jasmine.any(Function));
        });
    });
});

// Mock for testing purposes
var InsertQueryMock = function () {
    var self = this;
    this.createSpy = function () {
        return function () {
            self.spy = jasmine.createSpyObj('InsertQuery', ['insertValues', 'usingOptions', 'toString', 'execute']);
            self.spy.insertValues.and.returnValue('INSERT');
            self.spy.usingOptions.and.returnValue('USING');
            self.spy.toString.and.returnValue('INSERT USING');
            return self.spy;
        };

    };
};

module.exports = new InsertQueryMock();