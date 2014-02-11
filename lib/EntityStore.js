var Class    = require('js-class'),
    _        = require('underscore'),
    elements = require('evo-elements'),
    Errors = elements.Errors,
    Logger = elements.Logger,
    Parititioner = require('evo-idioms').Partitioner;

function makeEntity(id, data) {
    return {
        id: id,
        part: Parititioner.part(id),
        data: data
    };
}

var ENTITY_ATTRS = {
    id:    'string',
    part:  'integer',
    rev:   'string',
    ctime: 'date',
    mtime: 'date',
    data:  { nullable: 'object' }
};

function normalizeQueryResult(result) {
    var args = Object.keys(ENTITY_ATTRS).slice();
    args.unshift(result);
    return _.pick.apply(_, args);
}

var EntityStore = Class({
    constructor: function (engine, logger) {
        this.engine = engine;
        this.logger = Logger.clone(logger, { prefix: '<entities> ' });
    },

    create: function (type, id, data, done) {
        var entity = makeEntity(id, data);
        this.engine.insert(type, entity, function (err) {
            done(err, err ? undefined : normalizeQueryResult(entity));
        });
        return this;
    },

    update: function (type, id, rev, data, done) {
        this.engine.update(type, id, rev, data, function (err, result) {
            done(err, result && _.pick(result, 'mtime', 'rev'));
        });
        return this;
    },

    select: function (type, method, keys, opts, done) {
        var queryOpts = {};
        if (typeof(opts) == 'function') {
            done = opts;
            opts = {};
        }
        var filterAttrs = opts && opts.attrs ? opts.attrs : Object.keys(ENTITY_ATTRS);
        queryOpts.attrs = { id: true };
        if (Array.isArray(filterAttrs)) {
            filterAttrs.forEach(function (attr) {
                ENTITY_ATTRS[attr] && (queryOpts.attrs[attr] = true);
            });
        } else if (typeof(filterAttrs) == 'object') {
            for (var attr in filterAttrs) {
                ENTITY_ATTRS[attr] && filterAttrs[attr] && (queryOpts.attrs[attr] = true);
            }
        }
        switch (method) {
            case 'id':
                this.engine.fetch(type, keys, queryOpts, function (err, results) {
                    done(err, err ? undefined : results.map(function (obj) {
                        return normalizeQueryResult(obj);
                    }));
                });
                break;
            case 'part':
                this.engine.queryPartitions(type, parseInt(keys[0]), parseInt(keys[1]), queryOpts, function (err, results) {
                    done(err, err ? undefined : results.map(function (obj) {
                        return normalizeQueryResult(obj);
                    }));
                });
                break;
            default:
                done(Errors.badParam('method'));
        }
        return this;
    },

    remove: function (type, ids, done) {
        Array.isArray(ids) || (ids = [ids]);
        this.engine.remove(type, ids, function (err) {
            done(err);
        });
        return this;
    }
});

module.exports = EntityStore;
