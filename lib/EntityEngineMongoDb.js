var Class = require('js-class'),
    async = require('async'),
    elements = require('evo-elements'),
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

var EntityEngine = Class({
    constructor: function (conf, logger) {
        this._uri = conf.uri;
        this._options = conf.options || { };
        if (!this._uri) {
            throw new Error('Invalid conf: no uri');
        }
        this._logger = Logger.clone(logger, { prefix: '<ee:mongodb> ' });
        this._options.server || (this._options.server = {});
        this._options.server.auto_reconnect = true;
    },

    insert: function (type, entity, done) {
        this._col(type, function (c) {
            entity.ctime = entity.mtime = new Date();
            c.insert(entity, done);
        }, done);
    },

    update: function (type, id, data, done) {
        this._col(type, function (c) {
            var changes = { mtime: new Date(), data: data };
            c.update({ id: id }, { $set: changes }, function (err) {
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
                self._logger.debug('OPEN-DB: ' + self._uri);
                self._openDb(next);
            },
            function (db, next) {
                async.each([{ index: { id: 1 }, opts: { unique: true } },
                            { index: { part: 1 }, opts: { unique: false } }], function (index, next) {
                    db.createIndex(name, index.index, index.opts, function (err) {
                        err && self._logger.warn('INDEX-ERR: %s %j', Object.keys(index.index)[0], err);
                        next();
                    });
                }, function () {
                    next(null, db.collection(name));
                });
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

module.exports = function (conf, logger) {
    return new EntityEngine(conf, logger);
};
