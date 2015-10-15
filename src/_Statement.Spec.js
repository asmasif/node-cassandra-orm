var rewire = require('rewire');
var _Statement = rewire('./_Statement.js');

describe('lib/cassandra/_Statement.js', function () {

    describe('select (keyspace, table, columns, readable)', function () {
        it('should throw LIB_CAS__ST_1 if <columns> is passed and it is not an array', function () {
            expect(function () {
                _Statement.select('keyspace', 'table', 'key2');
            }).toThrowError('LIB_CAS__ST_1');
            expect(function () {
                _Statement.select('keyspace', 'table', {key: 'value'});
            }).toThrowError('LIB_CAS__ST_1');
        });
        it('should throw LIB_CAS__ST_2 if empty <columns> is passed', function () {
            expect(function () {
                _Statement.select('keyspace', 'table', []);
            }).toThrowError('LIB_CAS__ST_2');
        });
        it('should throw LIB_CAS__ST_3 if <readable> is passed and it is not an array', function () {
            expect(function () {
                _Statement.select('keyspace', 'table', ['key'], 'key');
            }).toThrowError('LIB_CAS__ST_3');
        });
        it('should throw LIB_CAS__ST_4 if empty <readable> is passed', function () {
            expect(function () {
                 _Statement.select('keyspace', 'table', ['key'], []);
            }).toThrowError('LIB_CAS__ST_4');
        });
        it('should throw LIB_CAS__ST_5 if unreadable <column> is passed', function () {
            expect(function () {
                 _Statement.select('keyspace', 'table', ['key'], ['key2']);
            }).toThrowError('LIB_CAS__ST_5');
        });
        it('should pass if valid or no <columns> are passed', function () {
            expect(_Statement.select('keyspace', 'table', ['key1'], ['key1', 'key2'])).toEqual('SELECT key1 FROM keyspace.table');
            //expect(_Statement.select('keyspace', 'table', void 0, ['key1'])).toEqual('SELECT key1 FROM keyspace.table');
            //expect(_Statement.select('keyspace', 'table')).toEqual('SELECT * FROM keyspace.table');
        });
    });
    describe('where (filters, queryParams)', function () {
        it('should throw LIB_CAS__ST_6 if <filters> is not a {}', function () {
            expect(function () {
                _Statement.where();
            }).toThrowError('LIB_CAS__ST_6');
            expect(function () {
                _Statement.where([], []);
            }).toThrowError('LIB_CAS__ST_6');
            expect(function () {
                _Statement.where(null, []);
            }).toThrowError('LIB_CAS__ST_6');
            expect(function () {
                _Statement.where('*', []);
            }).toThrowError('LIB_CAS__ST_6');
        });
        it('should pass if <clauses> have proper values and should return string with quote', function () {
            var queryParams = [];
            expect(_Statement.where({
                key1: 1,
                key2: [2,3]
            }, queryParams)).toEqual('WHERE key1 = ? AND key2 IN (?, ?)');
            expect(queryParams).toEqual([1,2,3]);
            queryParams = [];
            expect(_Statement.where({
                key1: 1.2
            }, queryParams)).toEqual('WHERE key1 = ?');
            expect(queryParams).toEqual([1.2]);
            queryParams = [];
            expect(_Statement.where({
                key1: 'Hello'
            }, queryParams)).toEqual('WHERE key1 = ?');
            expect(queryParams).toEqual(['Hello']);
        });
    });
    describe('orderBy (fields, key, direction)', function () {
        it('should throw LIB_CAS__ST_14 if <direction> is passed and not ASC or DESC', function () {
            expect(function () {
                _Statement.orderBy('key', 'DSC');
            }).toThrowError('LIB_CAS__ST_14');
        });
        it('should throw LIB_CAS__ST_14 if <direction> is not passed', function () {
            expect(function () {
                _Statement.orderBy('key');
            }).toThrowError('LIB_CAS__ST_14');
        });
        it('should pass if valid arguments were provided', function () {
            expect(_Statement.orderBy('key', 'DESC')).toEqual('ORDER BY key DESC');
            expect(_Statement.orderBy('key1', 'ASC')).toEqual('ORDER BY key1 ASC');
        });
    });
    describe('limit (size)', function () {
        it('should throw LIB_CAS__ST_15 if <size> is not a positive integer or whole numeric string', function () {
            expect(function () {
                _Statement.limit('1a');
            }).toThrowError('LIB_CAS__ST_15');
            expect(function () {
                _Statement.limit(-2);
            }).toThrowError('LIB_CAS__ST_15');
        });
        it('should pass if proper size was provided', function () {
            expect(_Statement.limit(10)).toEqual('LIMIT 10');
        });
    });
    describe('insertValues (keyspace, table, insertMap, queryParams)', function () {
        it('should throw LIB_CAS__ST_16 if insertMap is not a non-empty {}', function () {
            expect(function () {
                _Statement.insertValues('keyspace', 'table', 'key2');
            }).toThrowError('LIB_CAS__ST_16');
            expect(function () {
                _Statement.insertValues('keyspace', 'table', []);
            }).toThrowError('LIB_CAS__ST_16');
        });

        it('should pass if it has all required fields', function () {
            var queryParams = [];
            expect(_Statement.insertValues('keyspace', 'table', {key1: 10, key2: 20}, queryParams))
                .toEqual('INSERT INTO keyspace.table (key1, key2) VALUES (?, ?)');
            expect(queryParams).toEqual([10, 20]);
            queryParams = [];
            expect(_Statement.insertValues('keyspace', 'table', {key1: 10, key2: 20, key3: null}, queryParams))
                .toEqual('INSERT INTO keyspace.table (key1, key2, key3) VALUES (?, ?, ?)');
            expect(queryParams).toEqual([10, 20, null]);
        });
    });
    describe('usingOptions (options)', function () {
        it('should throw LIB_CAS__ST_21 if <options> is not a non-empty {}', function () {
            expect(function () {
                _Statement.usingOptions();
            }).toThrowError('LIB_CAS__ST_21');
            expect(function () {
                _Statement.usingOptions([]);
            }).toThrowError('LIB_CAS__ST_21');
            expect(function () {
                _Statement.usingOptions(null);
            }).toThrowError('LIB_CAS__ST_21');
            expect(function () {
                _Statement.usingOptions({});
            }).toThrowError('LIB_CAS__ST_21');
            expect(function () {
                _Statement.usingOptions('*');
            }).toThrowError('LIB_CAS__ST_21');
        });
        it('should throw LIB_CAS__ST_22 if <options> is not within valid set', function () {
            expect(function () {
                _Statement.usingOptions({
                    random: 10
                });
            }).toThrowError('LIB_CAS__ST_22');
        });
        it('should pass if proper options are provided', function () {
            expect(_Statement.usingOptions({
                ttl: 86400
            })).toEqual('USING TTL 86400');
            expect(_Statement.usingOptions({
                TTL: 86400,
                timestamp: 1000
            })).toEqual('USING TTL 86400 AND TIMESTAMP 1000');
        });
    });
    describe('set (nameValueMap, queryParams, updateRestrictions)', function () {
        it('should throw LIB_CAS__ST_31 if less than 2 arguments are provided', function () {
            expect(function () {
                _Statement.set();
            }).toThrowError('LIB_CAS__ST_31');
            expect(function () {
                _Statement.set({
                    key: 1
                });
            }).toThrowError('LIB_CAS__ST_31');
        });
        it('should throw LIB_CAS__ST_23 if <nameValueMap> is not a non-empty {}', function () {
            expect(function () {
                _Statement.set({}, []);
            }).toThrowError('LIB_CAS__ST_23');
            expect(function () {
                _Statement.set('*', []);
            }).toThrowError('LIB_CAS__ST_23');
        });
        it('should throw LIB_CAS__ST_26 if restricted key is specified in <nameValueMap>', function () {
            expect(function () {
                _Statement.set({
                    key2: 10
                }, [], ['key1']);
            }).toThrowError('LIB_CAS__ST_26');
        });
        it('should pass if proper arguments are provided', function () {
            var queryParams = [];
            expect(_Statement.set({
                key1: 10,
                key2: 'Hi'
            }, queryParams, ['key1', 'key2'])).toEqual('SET key1 = ?, key2 = ?');
            expect(queryParams).toEqual([10,'Hi']);
        });
    });
    describe('update (keyspace, table)', function () {
        it('should generate clause for valid arguments', function () {
            expect(_Statement.update('keyspace', 'table')).toEqual('UPDATE keyspace.table');
        });
    });
    describe('delete (keyspace, table)', function () {
        it('should pass if proper arguments are provided', function () {
            expect(_Statement.delete('keyspace', 'table'))
                .toEqual('DELETE FROM keyspace.table');
        });
    });
});

// Mock for testing purposes
var StatementMock = function () {
    this.createSpy = function () {
        var spy = jasmine.createSpyObj('_Statement', ['select', 'where', 'orderBy', 'limit',
            'insertValues', 'usingOptions', 'update', 'set', 'delete', 'execute']);

        spy.select.and.returnValue('SELECT');
        spy.where.and.returnValue('WHERE');
        spy.orderBy.and.returnValue('ORDER');
        spy.limit.and.returnValue('LIMIT');
        spy.insertValues.and.returnValue('INSERT');
        spy.usingOptions.and.returnValue('USING');
        spy.update.and.returnValue('UPDATE');
        spy.set.and.returnValue('SET');
        spy.delete.and.returnValue('DELETE');

        return spy;
    };
};

module.exports = new StatementMock();