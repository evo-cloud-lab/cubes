var Class  = require('js-class'),
    assert = require('assert'),
    Try    = require('evo-elements').Try,
    Errors = require('evo-elements').Errors,
    Partitioner = require('evo-idioms').Partitioner,

    EntityStore = require('../lib/EntityStore');

describe('EntityStore', function () {
    it('#create', function (done) {
        var args = [];
        var store = new EntityStore({
            insert: function (type, entity, callback) {
                args.push({ type: type, entity: entity });
                callback(null, 1234);
            }
        });

        store.create('type', 'id', { key: 'value' }, function (err, entity) {
            Try.final(function () {
                assert.equal(err, null);
                assert.deepEqual(args, [{ type: 'type', entity: { id: 'id', part: Partitioner.part('id'), data: { key: 'value' } } }]);
                assert.deepEqual(entity, { id: 'id', part: Partitioner.part('id'), data: { key: 'value' } });
            }, done);
        });
    });

    it('#update', function (done) {
        var args = [];
        var store = new EntityStore({
            update: function (type, id, rev, data, callback) {
                args.push({ type: type, id: id, rev: rev, data: data });
                callback(null, {});
            }
        });

        store.update('type', 'id', 'rev', { key: 'value' }, {}, function (err) {
            Try.final(function () {
                assert.equal(err, null);
                assert.deepEqual(args, [{ type: 'type', id: 'id', rev: 'rev', data: { key: 'value' } }]);
            }, done);
        });
    });

    it('#update conflict', function (done) {
        var store = new EntityStore({
            update: function (type, id, rev, data, callback) {
                callback(Errors.nonexist(id, { rev: rev }));
            }
        });

        store.update('type', 'id', 'rev', { key: 'value' }, {}, function (err) {
            Try.final(function () {
                assert.ok(err);
                assert.equal(err.code, 'NONEXIST');
                assert.equal(err.id, 'id');
                assert.equal(err.rev, 'rev');
            }, done);
        });
    });

    it('#update conflict refresh', function (done) {
        var args = [];
        var store = new EntityStore({
            update: function (type, id, rev, data, callback) {
                callback(Errors.nonexist(id, { rev: rev }));
            },

            fetch: function (type, ids, opts, callback) {
                args.push({ type: type, ids: ids });
                callback(null, ids.map(function (id) { return { id: id, part: 'part', data: 'test', rev: 'rev1' }; }));
            }
        });

        store.update('type', 'id', 'rev', { key: 'value' }, { refresh: true }, function (err) {
            Try.final(function () {
                assert.deepEqual(args, [{ type: 'type', ids: ['id'] }]);
                assert.ok(err);
                assert.equal(err.code, 'CONFLICT');
                assert.equal(err.id, 'id');
                assert.equal(err.rev, 'rev');
                assert.deepEqual(err.entity, { id: 'id', part: 'part', data: 'test', rev: 'rev1' });
            }, done);
        });
    });

    it('#update conflict refresh nonexists', function (done) {
        var store = new EntityStore({
            update: function (type, id, rev, data, callback) {
                callback(Errors.nonexist(id, { rev: rev }));
            },

            fetch: function (type, ids, opts, callback) {
                callback(null, []);
            }
        });

        store.update('type', 'id', 'rev', { key: 'value' }, { refresh: true }, function (err) {
            Try.final(function () {
                assert.ok(err);
                assert.equal(err.code, 'NONEXIST');
                assert.equal(err.id, 'id');
                assert.equal(err.rev, null);
            }, done);
        });
    });

    it('#update conflict refresh fail', function (done) {
        var store = new EntityStore({
            update: function (type, id, rev, data, callback) {
                callback(Errors.nonexist(id, { rev: rev }));
            },

            fetch: function (type, ids, opts, callback) {
                callback(Errors.make('TEST'));
            }
        });

        store.update('type', 'id', 'rev', { key: 'value' }, { refresh: true }, function (err) {
            Try.final(function () {
                assert.ok(err);
                assert.equal(err.code, 'TEST');
            }, done);
        });
    });

    it('#select id', function (done) {
        var args = [];
        var store = new EntityStore({
            fetch: function (type, ids, opts, callback) {
                args.push({ type: type, ids: ids });
                callback(null, ids.map(function (id) { return { id: id, part: 'part', data: 'test', key: 'value' }; }));
            }
        });

        store.select('type', 'id', ['id1', 'id2'], function (err, entities) {
            Try.final(function () {
                assert.equal(err, null);
                assert.deepEqual(args, [{ type: 'type', ids: ['id1', 'id2'] }]);
                assert.deepEqual(entities, [{ id: 'id1', part: 'part', data: 'test'}, { id: 'id2', part: 'part', data: 'test' }]);
            }, done);
        });
    });

    it('#select part', function (done) {
        var args = [];
        var store = new EntityStore({
            queryPartitions: function (type, part, count, opts, callback) {
                args.push({ type: type, part: part, count: count });
                results = [];
                for (var i = 0; i < count; i ++) {
                    results.push({
                        id: 'id' + i,
                        part: part + i,
                        data: 'test',
                        key: 'value'
                    });
                }
                callback(null, results);
            }
        });

        store.select('type', 'part', [100, 2], function (err, entities) {
            Try.final(function () {
                assert.equal(err, null);
                assert.deepEqual(args, [{ type: 'type', part: 100, count: 2 }]);
                assert.deepEqual(entities, [{ id: 'id0', part: 100, data: 'test' }, { id: 'id1', part: 101, data: 'test' }]);
            }, done);
        });
    });

    it('#select other', function (done) {
        var store = new EntityStore({});
        store.select('type', 'other', [], function (err, entities) {
            Try.final(function () {
                assert.ok(err);
                assert.equal(err.code, 'BADPARAM');
                assert.equal(err.param, 'method');
            }, done);
        });
    });

    it('#remove single id', function (done) {
        var args = [];
        var store = new EntityStore({
            remove: function (type, ids, callback) {
                args.push({ type: type, ids: ids });
                callback();
            }
        });

        store.remove('type', 'id', function (err) {
            Try.final(function () {
                assert.equal(err, null);
                assert.deepEqual(args, [{ type: 'type', ids: ['id'] }]);
            }, done);
        });
    });

    it('#remove ids', function (done) {
        var args = [];
        var store = new EntityStore({
            remove: function (type, ids, callback) {
                args.push({ type: 'type', ids: ids });
                callback();
            }
        });

        store.remove('type', ['id1', 'id2'], function (err) {
            Try.final(function () {
                assert.equal(err, null);
                assert.deepEqual(args, [{ type: 'type', ids: ['id1', 'id2'] }]);
            }, done);
        });
    });
});
