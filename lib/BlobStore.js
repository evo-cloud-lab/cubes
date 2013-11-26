var Class = require('js-class'),
    path  = require('path'),
    fs    = require('fs'),
    async = require('async'),
    elements = require('evo-elements'),
    BiMap  = elements.BiMap,
    Logger = elements.Logger,
    Try    = elements.Try;

var _opId = 0;

var Op = Class(process.EventEmitter, {
    constructor: function (engineOp) {
        this.id = ++ _opId;
        this.engineOp = engineOp;
        this._progress = 0;

        engineOp
            .on('progress', this.onProgress.bind(this))
            .on('complete', this.onComplete.bind(this))
        ;
    },

    get progress () {
        return this._progress;
    },

    abort: function () {
        if (this.engineOp) {
            this.engineOp.abort();
        }
    },

    onProgress: function (percentage) {
        if (this.engineOp) {
            this._progress = percentage;
            this.emit('progress', percentage, this);
        }
    },

    onComplete: function (err) {
        if (this.engineOp) {
            delete this.engineOp;
            this.emit('complete', err, this);
        }
    }
});

var BlobRef = Class({
    constructor: function (id, cacheFile, op) {
        this._id = id;
        this._file = cacheFile;
        this._op = op;
        this._update();
        if (op) {
            op
                .on('progress', this.onProgress.bind(this))
                .on('complete', this.onComplete.bind(this))
            ;
        }
    },

    get id () {
        return this._id;
    },

    get state () {
        return this._state;
    },

    get op () {
        return this._op;
    },

    onProgress: function () {
        if (this._op) {
            this._update();
        }
    },

    onComplete: function (err) {
        if (this._op) {
            this._error = err;
            err || fs.writeFile(this._file + '.ready', '');
            delete this._op;
            this._update();
        }
    },

    _update: function () {
        this._state = { file: this._file };
        if (this._op) {
            this._state.opId = this._op.id;
            this._state.progress = this._op.progress;
        } else if (this._error) {
            this._state.error = true;
        } else {
            this._state.ready = true;
        }
    }
});

var BlobStore = Class(process.EventEmitter, {
    constructor: function (engine, conf, logger) {
        this.engine = engine;
        this.logger = Logger.clone(logger, { prefix: '<blobs> ' });
        this._cachedir = path.resolve(process.cwd(), conf.cachedir || '_blobcache');
        this._ops = {};
        this._subs = new BiMap('op', 'client');
        this._blobs = {};
        this._refs = new BiMap('blob', 'client');
    },

    upload: function (clientId, localFile, done) {
        this.engine.upload(localFile, Try.br(function (blobId, engineOp) {
            done(null, { blobId: blobId, opId: this._addOp(engineOp, clientId).id });
        }.bind(this), done));
        return this;
    },

    info: function (blobIds, callback) {
        this.engine.info(blobIds, callback);
        return this;
    },

    remove: function (blobIds, callback) {
        this.engine.remove(blobIds, callback);
        // TODO clear cache
        return this;
    },

    request: function (clientId, blobId, done) {
        var cacheFile = this._cacheFile(blobId);
        var ref = this._refBlob(blobId, clientId, function () {
            return fs.existsSync(cacheFile + '.ready') ? new BlobRef(blobId, cacheFile) : null;
        });
        if (ref) {
            done(null, ref.blob.state);
        } else {
            this.engine.download(blobId, cacheFile, Try.br(function (engineOp) {
                var op = this._addOp(engineOp);
                this._blobs[blobId] = blob = new BlobRef(blobId, cacheFile, op);
                this._refBlob(blobId, clientId);
                op.on('complete', function (err) {
                    if (err) {
                        this._unloadBlob(blobId);
                    }
                }.bind(this));
                done(null, blob.state);
            }.bind(this), done));
        }
        return this;
    },

    release: function (clientId, blobId, done) {
        var ref = this._refs.get(blobId, clientId);
        if (ref && ref.refs == 1) {
            this._refs.remove(blobId, clientId);
            if (ref.blob.op) {
                this._subs.remove(ref.blob.op.id, clientId);
            }
        }
        done();
        return this;
    },

    listOps: function (clientId, done) {
        done(null, this._subs.keys(clientId, 'client').map(function (opId) {
            var op = this._ops[opId];
            return op ? { id: opId, progress: op.progress } : null;
        }.bind(this)));
        return this;
    },

    abort: function (opId) {
        var op = this._ops[opId];
        op && op.abort();
        return this;
    },

    disconnect: function (clientId) {
        this._subs.removeAll(clientId, 'client');
        this._refs.removeAll(clientId, 'client');
        return this;
    },

    cleanup: function (delay) {
        setTimeout(this._clean.bind(this), delay || 0);
        return this;
    },

    _cacheFile: function (blobId) {
        return path.join(this._cachedir, blobId.substr(0, 2) + '#', '#' + blobId.substr(2));
    },

    _addOp: function (engineOp, clientId) {
        var op = new Op(engineOp);
        (this._ops[op.id] = op)
            .on('progress', this.opProgress.bind(this))
            .on('complete', this.opComplete.bind(this))
        ;
        clientId != null && this._subs.add(op.id, clientId);
        return op;
    },

    _refBlob: function (blobId, clientId, loadBlobFn) {
        var ref = this._refs.get(blobId, clientId);
        if (ref) {
            ref.refs ++;
        } else {
            var blob = this._blobs[blobId];
            if (!blob && loadBlobFn) {
                blob = this._blobs[blobId] = loadBlobFn(blobId);
            }
            if (blob) {
                ref = { refs: 1, blob: blob };
                this._refs.add(blobId, clientId, ref);
                if (blob.op) {
                    this._subs.add(blob.op.id, clientId);
                }
            }
        }
        return ref;
    },

    _unloadBlob: function (blobId) {
        this._refs.removeAll(blobId, 'blob');
        delete this._blobs[blobId];
    },

    _clean: function () {
        if (this._cleaning) {
            this._cleaning ++;
            return;
        } else {
            this._cleaning = 1;
        }
        var done = function (err) {
            if (err || -- this._cleaning == 0) {
                delete this._cleaning;
            } else {
                setTimeout(this._clean.bind(this), 0);
            }
        }.bind(this);

        var self = this;
        async.waterfall([
            function (next) {
                fs.readdir(self._cachedir, next);
            },
            function (files, next) {
                async.map(files.filter(function (name) {
                    return name.length == 3 && name[2] == '#';
                }), function (dir, next) {
                    async.waterfall([
                        function (next) {
                            fs.readdir(path.join(self._cachedir, dir), next);
                        },
                        function (files, next) {
                            async.filter(files.filter(function (name) {
                                return name[0] == '#' && name.indexOf('.') < 0;
                            }), function (name, next) {
                                fs.stat(path.join(self._cachedir, dir, name), function (err, stats) {
                                    next(!err && stats.isFile());
                                });
                            }, next);
                        }
                    ], function (err, files) {
                        next(null, !err && files ? files.map(function (name) { return dir.substr(0, 2) + name.substr(1); }) : []);
                    });
                }, next);
            },
            function (dirs, next) {
                next(null, [].concat.apply([], dirs));
            }
        ], Try.br(function (blobIds) {
            var blobMap = this._refs.map('blob');
            var cleanIds = blobIds.filter(function (id) { return !blobMap[id]; });
            async.each(cleanIds, function (blobId, next) {
                var fn = self._cacheFile(blobId);
                async.series([
                    function (next) {
                        fs.unlink(fn + '.ready', next);
                    },
                    function (next) {
                        fs.unlink(fn, next);
                    }
                ], function () { next(); });
            }, done);
        }, done));
    },

    opProgress: function (percentage, op) {
        this.emit('progress', op.id, percentage, this._subs.keys(op.id, 'op'));
    },

    opComplete: function (err, op) {
        var clients = this._subs.keys(op.id, 'op');
        this._subs.removeAll(op.id, 'op');
        delete this._ops[op.id];
        this.emit('complete', op.id, err, clients);
    }
});

module.exports = BlobStore;
