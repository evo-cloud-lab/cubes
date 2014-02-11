var Class = require('js-class'),
    async = require('async'),
    uuid  = require('uuid'),
    elements = require('evo-elements'),
    Errors = elements.Errors,
    Try    = elements.Try,
    Logger = elements.Logger,
    MongoClient = require('mongodb').MongoClient;

function queryOptions(opts) {
    var findOpts = {};
    if (opts.attrs) {
        findOpts.fields = {};
        for (var attr in opts.attrs) {
            opts.attrs[attr] && (findOpts.fields[attr] = 1);
        }
    }
    return findOpts;
}

function newRev() {
    return uuid.v4().replace(/-/g, '');
}

var MongoEngine = Class({
    constructor: function (conf, logger) {
        this._options = conf.options || { };
        this._logger = logger;
    },

    insert: function (type, entity, done) {
        this._col(type, function (c) {
            entity.ctime = entity.mtime = new Date();
            entity.rev = newRev();
            c.insert(entity, done);
        }, done);
    },

    update: function (type, id, rev, data, done) {
        this._col(type, function (c) {
            var changes = { mtime: new Date(), rev: newRev(), data: data };
            c.update({ id: id, rev: rev }, { $set: changes }, function (err, count) {
                if (!err && count < 1) {
                    err = Errors.nonexist(id, { rev: rev });
                }
                done(err, changes);
            });
        }, done);
    },

    fetch: function (type, ids, opts, done) {
        this._col(type, function (c) {
            c.find({ id: { $in: ids } }, queryOptions(opts), function (err, results) {
                err ? done(err) : results.toArray(done);
            });
        }, done);
    },

    queryPartitions: function (type, part, count, opts, done) {
        this._col(type, function (c) {
            c.find({ $and: [
                    { part: { $gte: part } },
                    { part: { $lt: part + count } }
                ] }, queryOptions(opts), function (err, results) {
                err ? done(err) : results.toArray(done);
            });
        }, done);
    },

    remove: function (type, ids, done) {
        this._col(type, function (c) {
            c.remove({ id: { $in: ids } }, done);
        }, done);
    },

    _col: function (name, success, failure) {
        var self = this;
        async.waterfall([
            function (next) {
                self._openDb(next);
            },
            function (db, next) {
                async.each([
                        { index: { id: 1 }, opts: { unique: true } },
                        { index: { part: 1 }, opts: { unique: false } },
                        { index: { mtime: 1 }, opts: { unique: false } }
                    ],
                    function (index, next) {
                        db.createIndex(name, index.index, index.opts, function (err) {
                            err && self._logger.warn('INDEX-ERR: %s %j', Object.keys(index.index)[0], err);
                            next();
                        });
                    },
                    function () {
                        next(null, db.collection(name));
                    }
                );
            }
        ], Try.br(success, failure));
    },

    _openDb: function (callback) {
        if (this._db) {
            callback(null, this._db);
        } else {
            MongoClient.connect(this._uri, this._options, function (err, db) {
                err || (this._db = db);
                callback(err, db);
            }.bind(this));
        }
    }
});

module.exports = MongoEngine;
