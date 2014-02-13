var Class = require('js-class'),
    async = require('async'),
    path  = require('path'),
    fs    = require('fs'),
    uuid  = require('uuid'),
    mkdir = require('mkdirp'),
    spawn = require('child_process').spawn,
    elements = require('evo-elements'),
    Errors = elements.Errors,
    Logger = elements.Logger,
    Try    = elements.Try;

function blobId2Dirs(blobId) {
    return { dir: blobId.substr(0, 2), file: blobId.substr(2) };
}

var CopyOp = Class(process.EventEmitter, {
    constructor: function (src, dst) {
        this._src = src;
        this._dst = dst;
        (this._proc = spawn('cp', [src, dst], { stdio: 'ignore' }))
            .on('error', this.onError.bind(this))
            .on('exit', this.onExit.bind(this));
        this.emit('progress', 0);
    },

    abort: function () {
        this._aborting = true;
        if (this._proc) {
            this._proc.kill('SIGKILL');
        }
        return this;
    },

    onError: function (err) {
        if (this._proc) {
            delete this._proc;
            this.emit('complete', err);
        }
    },

    onExit: function (code, signal) {
        if (this._proc) {
            delete this._proc;
            if (code == 0) {
                this.emit('progress', 100);
                this.emit('complete');
            } else {
                fs.unlink(this._dst, function () {
                    var err;
                    if (this._aborting) {
                        err = Errors.aborted();
                    } else {
                        err = Errors.procExit(code, signal);
                    }
                    this.emit('complete', err);
                }.bind(this));
            }
        }
    }
});

var BlobEngine = Class({
    constructor: function (conf, logger) {
        this._basedir = path.resolve(process.cwd(), conf.path || '_blobs');
        this._logger = Logger.clone(logger, { prefix: '<be:fs> ' });
    },

    upload: function (localFile, callback) {
        var blobId = uuid.v4().replace(/-/g, '');
        var dirs = blobId2Dirs(blobId);
        var dir = path.join(this._basedir, dirs.dir);
        var file = path.join(dir, dirs.file);
        mkdir(dir, function (err) {
            err ? callback(err) : callback(null, blobId, new CopyOp(localFile, file));
        });
    },

    info: function (blobIds, callback) {
        async.map(blobIds, function (blobId, next) {
            var dirs = blobId2Dirs(blobId);
            var blobFile = path.join(this._basedir, dirs.dir, dirs.file);
            fs.stat(blobFile, function (err, stat) {
                next(null, !err && stat ? { size: stat.size, ctime: stat.ctime, mtime: stat.mtime } : null);
            });
        }.bind(this), callback);
    },

    download: function (blobId, localFile, callback) {
        var dirs = blobId2Dirs(blobId);
        var blobFile = path.join(this._basedir, dirs.dir, dirs.file);
        fs.exists(blobFile, function (exists) {
            exists ? mkdir(path.dirname(localFile), function (err) {
                callback(err, err ? undefined : new CopyOp(blobFile, localFile));
            }) : callback(Errors.nonexist(blobId));
        });
    },

    remove: function (blobIds, callback) {
        async.each(blobIds, function (blobId, next) {
            var dirs = blobId2Dirs(blobId);
            var blobFile = path.join(this._basedir, dirs.dir, dirs.file);
            fs.unlink(blobFile, function () { next(); });
        }.bind(this), callback);
    }
});

module.exports = function (data) {
    return new BlobEngine(data.conf, data.logger);
};
