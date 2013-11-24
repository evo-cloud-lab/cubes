var Class = require('js-class'),
    async = require('async'),
    Try   = require('evo-elements').Try,
    MongoClient = require('mongodb').MongoClient;

var EntityEngine = Class({
    constructor: function (conf) {
        this._uri = conf.uri;
        this._options = conf.options || { };
        if (!this._uri) {
            throw new Error('Invalid conf: no uri');
        }
        this._options.server || (this._options.server = {});
        this._options.server.auto_reconnect = true;
    },

    insert: function (type, entity, done) {
        this.col(type, function (col) {
            col.insert(entity, done);
        }, done);
    },

    update: function (type, id, data, done) {
        this.col(type, function (col) {
            col.update({ id: id }, { $set: { data: data } }, done);
        }, done);
    },

    fetch: function (type, ids, done) {
        this.col(function (col) {
            col.find({ id: { $in: ids } }, done);
        }, done);
    },

    queryPartitions: function (type, part, count, done) {
        this.col(function (col) {
            col.find({ $and: [
                    { part: { $gte: part } },
                    { part: { $lt: part + count } }
                ] }, { fields: { id: 1 } }, done);
        }, done);
    },

    remove: function (type, ids, done) {
        this.col(function (col) {
            col.remove({ id: { $in: ids } }, done);
        }, done);
    },

    col: function (name, success, failure) {
        var self = this, collection;
        async.waterfall([
            function (next) {
                self._openDb(next);
            },
            function (db, next) {
                db.collection(name, next);
            },
            function (col, next) {
                collection = col;
                collection.ensureIndex({ id: 1 }, { unique: true }, next);
            },
            function (indexName, next) {
                collection.ensureIndex({ part: 1 }, { unique: false }, next);
            },
            function (indexName, next) {
                next(null, collection);
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

module.exports = function (conf) {
    return new EntityEngine(conf);
};
