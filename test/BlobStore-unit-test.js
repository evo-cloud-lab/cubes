var Class  = require('js-class'),
    assert = require('assert'),
    Try    = require('evo-elements').Try,
    sandbox = require("sandboxed-module"),

    BlobStore = require('../lib/BlobStore');

describe('BlobStore', function () {
    var StubEngineOp = Class(process.EventEmitter, {
        constructor: function (magic) {
            this.magic = magic;
        }
    });

    function sandboxBlobStore(fsStub) {
        return sandbox.require('../lib/BlobStore', {
            requires: {
                fs: fsStub
            }
        });
    }

    it('#upload create Op', function (done) {
        var uploadArgs = [], events = [];
        var engineOp = new StubEngineOp(1234);
        var store = new BlobStore({
            upload: function (localFile, callback) {
                uploadArgs.push(localFile);
                callback(null, engineOp);
            }
        }, {});
        store.on('progress', function (opId, progress, clients) {
            events.push({
                opId: opId,
                progress: progress,
                clients: clients
            });
        });
        store.upload('client', 'localFile', function (err, state) {
            Try.final(function () {
                assert.deepEqual(uploadArgs, ['localFile']);
                assert.equal(err, null);
                assert.ok(state.opId);
                engineOp.emit('progress', 10);
                assert.deepEqual(events, [{ opId: state.opId, progress: 10, clients: ['client'] } ]);
            }, done);
        });
    });

    it('#info passthrough', function (done) {
        var ids;
        var store = new BlobStore({
            info: function (blobIds, callback) {
                ids = blobIds;
                callback(null, [1, 2, 3]);
            }
        }, {});
        store.info(['a', 'b', 'c'], function (err, results) {
            Try.final(function () {
                assert.deepEqual(ids, ['a', 'b', 'c']);
                assert.deepEqual(results, [1, 2, 3]);
            }, done);
        });
    });

    describe('#request', function () {
        it('load existing cache', function (done) {
            var paramsExistsSync = [];
            var SandboxedBlobStore = sandboxBlobStore({
                existsSync: function (filename) {
                    paramsExistsSync.push(filename);
                    return true;
                }
            });
            var store = new SandboxedBlobStore({}, { cachedir: '/cache' });
            store.request('client', 'blobId', function (err, state) {
                Try.tries(function () {
                    assert.equal(err, null);
                    assert.strictEqual(state.ready, true);
                    assert.strictEqual(state.file, '/cache/bl#/#obId');
                    assert.deepEqual(paramsExistsSync, ['/cache/bl#/#obId.ready']);

                    store.request('client1', 'blobId', function (err, state) {
                        Try.final(function () {
                            assert.equal(err, null);
                            assert.strictEqual(state.ready, true);
                            assert.strictEqual(state.file, '/cache/bl#/#obId');
                            assert.deepEqual(paramsExistsSync, ['/cache/bl#/#obId.ready']);
                        }, done);
                    });

                }, done);
            });
        });

        it('download blob', function (done) {
            var SandboxedBlobStore = sandboxBlobStore({
                existsSync: function () { return false; }
            });
            var downloadArgs = [], events = [], engineOp = new StubEngineOp(1234);
            var store = new SandboxedBlobStore({
                download: function (blobId, cacheFile, callback) {
                    downloadArgs.push({ id: blobId, file: cacheFile });
                    callback(null, engineOp);
                }
            }, { cachedir: '/cache' });
            store.on('progress', function (opId, progress, clients) {
                events.push({
                    opId: opId,
                    progress: progress,
                    clients: clients
                });
            });
            store.request('client', 'blobId', function (err, state) {
                Try.tries(function () {
                    assert.equal(err, null);
                    assert.equal(state.ready, null);
                    assert.strictEqual(state.file, '/cache/bl#/#obId');
                    assert.ok(state.opId);
                    assert.deepEqual(downloadArgs, [{ id: 'blobId', file: '/cache/bl#/#obId' }]);

                    engineOp.emit('progress', 10);
                    assert.deepEqual(events, [{ opId: state.opId, progress: 10, clients: ['client'] } ]);

                    store.request('client1', 'blobId', function (err, state1) {
                        Try.tries(function () {
                            assert.equal(err, null);
                            assert.equal(state1.ready, null);
                            assert.strictEqual(state1.file, '/cache/bl#/#obId');
                            assert.strictEqual(state1.opId, state.opId);
                            assert.deepEqual(downloadArgs, [{ id: 'blobId', file: '/cache/bl#/#obId' }]);

                            engineOp.emit('progress', 20);
                            assert.equal(events.length, 2);
                            assert.ok(events[1].clients.indexOf('client') >= 0);
                            assert.ok(events[1].clients.indexOf('client1') >= 0);

                            store.listOps('client', function (err, opIds) {
                                Try.tries(function () {
                                    assert.equal(err, null);
                                    assert.deepEqual(opIds, [{ id: state.opId, progress: 20 }]);

                                    store.listOps('client1', function (err, opIds) {
                                        Try.final(function () {
                                            assert.equal(err, null);
                                            assert.deepEqual(opIds, [{ id: state.opId, progress: 20 }]);
                                        }, done);
                                    });
                                }, done);
                            });

                        }, done);
                    });

                }, done);
            });
        });

        it('download blob failed', function (done) {
            var SandboxedBlobStore = sandboxBlobStore({
                existsSync: function () { return false; }
            });
            var events = [], engineOp = new StubEngineOp(1234);
            var store = new SandboxedBlobStore({
                download: function (blobId, cacheFile, callback) {
                    callback(null, engineOp);
                }
            }, { cachedir: '/cache' });
            store.on('complete', function (opId, err, clients) {
                events.push({
                    opId: opId,
                    error: err,
                    clients: clients
                });
            });
            store.request('client', 'blobId', function (err, state) {
                Try.tries(function () {
                    assert.equal(err, null);
                    assert.equal(state.ready, null);
                    assert.strictEqual(state.file, '/cache/bl#/#obId');
                    assert.ok(state.opId);

                    engineOp.emit('complete', new Error('test'));
                    assert.equal(events.length, 1);
                    assert.equal(events[0].opId, state.opId);
                    assert.ok(events[0].error);
                    assert.equal(events[0].error.message, 'test');
                    assert.deepEqual(events[0].clients, ['client']);

                    engineOp.emit('complete');
                    assert.equal(events.length, 1);

                    store.listOps('client', function (err, opIds) {
                        Try.final(function () {
                            assert.equal(err, null);
                            assert.equal(opIds.length, 0);
                        }, done);
                    });
                }, done);
            });
        });
    });

    it('#release', function (done) {
        var SandboxedBlobStore = sandboxBlobStore({
            existsSync: function () { return false; }
        });
        var events = [], engineOp = new StubEngineOp(1234);
        var store = new SandboxedBlobStore({
            download: function (blobId, cacheFile, callback) {
                callback(null, engineOp);
            }
        }, { cachedir: '/cache' });
        store.on('progress', function (opId, progress, clients) {
            events.push({
                opId: opId,
                progress: progress,
                clients: clients
            });
        });
        store.request('client', 'blobId', function (err, state) {
            Try.tries(function () {
                assert.equal(err, null);
                assert.equal(state.ready, null);
                assert.strictEqual(state.file, '/cache/bl#/#obId');
                assert.ok(state.opId);

                engineOp.emit('progress', 10);
                assert.deepEqual(events, [{ opId: state.opId, progress: 10, clients: ['client'] } ]);

                store.release('client', 'blobId', function (err) {
                    Try.final(function () {
                        assert.equal(err, null);
                        engineOp.emit('progress', 20);
                        assert.equal(events.length, 2);
                        assert.deepEqual(events[1].clients, []);
                    }, done);
                })
            }, done);
        });
    });

    it('#abort', function (done) {
        var SandboxedBlobStore = sandboxBlobStore({
            existsSync: function () { return false; }
        });
        var events = [], engineOp = new StubEngineOp(1234);
        engineOp.abort = function () {
            this.emit('complete', new Error('Aborted'));
        };
        var store = new SandboxedBlobStore({
            download: function (blobId, cacheFile, callback) {
                callback(null, engineOp);
            }
        }, { cachedir: '/cache' });
        store.on('complete', function (opId, err, clients) {
            events.push({
                opId: opId,
                error: err,
                clients: clients
            });
        });
        store.request('client', 'blobId', function (err, state) {
            Try.final(function () {
                assert.equal(err, null);
                assert.equal(state.ready, null);
                assert.strictEqual(state.file, '/cache/bl#/#obId');
                assert.ok(state.opId);

                store.abort(state.opId);

                assert.equal(events.length, 1);
                assert.equal(events[0].opId, state.opId);
                assert.ok(events[0].error);
                assert.equal(events[0].error.message, 'Aborted');
                assert.deepEqual(events[0].clients, ['client']);
            }, done);
        });
    });

    it('#disconnect', function (done) {
        var SandboxedBlobStore = sandboxBlobStore({
            existsSync: function () { return false; }
        });
        var events = [], engineOp = new StubEngineOp(1234);
        var store = new SandboxedBlobStore({
            download: function (blobId, cacheFile, callback) {
                callback(null, engineOp);
            }
        }, { cachedir: '/cache' });
        store.on('progress', function (opId, progress, clients) {
            events.push({
                opId: opId,
                progress: progress,
                clients: clients
            });
        });
        store.request('client', 'blobId', function (err, state) {
            Try.final(function () {
                assert.equal(err, null);
                assert.equal(state.ready, null);
                assert.strictEqual(state.file, '/cache/bl#/#obId');
                assert.ok(state.opId);

                store.disconnect('client');
                engineOp.emit('progress', 10);
                assert.deepEqual(events, [{ opId: state.opId, progress: 10, clients: [] }]);
            }, done);
        });
    });

    it('#cleanup');
});
