var Class = require('js-class'),
    Logger = require('evo-elements').Logger,
    MongoClient = require('mongodb').MongoClient,

    MongoEngine = require('./MongoEngine');

var EntityEngine = Class(MongoEngine, {
    constructor: function (conf, logger) {
        MongoEngine.prototype.constructor.call(this, conf, Logger.clone(logger, { prefix: '<ee:mongodb> ' }));
        this._uri = conf.uri;
        if (!this._uri) {
            throw new Error('Invalid conf: no uri');
        }
        this._options.server || (this._options.server = {});
        this._options.server.auto_reconnect = true;
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

module.exports = function (data, host, info, callback) {
    callback(null, new EntityEngine(data.conf, data.logger));
};
