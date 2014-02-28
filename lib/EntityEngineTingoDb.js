var Class = require('js-class'),
    Logger = require('evo-elements').Logger,
    tingodb = require('tingodb')();

    MongoEngine = require('./MongoEngine');

var EntityEngine = Class(MongoEngine, {
    constructor: function (conf, logger) {
        MongoEngine.prototype.constructor.call(this, conf, Logger.clone(logger, { prefix: '<ee:tingodb> ' }));
        this._file = conf.file;
        if (!this._file) {
            throw new Error('Invalid conf: no file');
        }
    },

    _openDb: function (callback) {
        if (!this._db) {
            this._db = new tingodb.Db(this._file);
        }
        callback(null, this._db);
    }
});

module.exports = function (data, host, info, callback) {
    callback(null, new EntityEngine(data.conf, data.logger));
};
