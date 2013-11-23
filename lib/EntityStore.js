var Class  = require('js-class'),
    Errors = require('evo-elements').Errors,

    Parititioner = require('./Partitioner');

function makeEntity(id, data, part) {
    return {
        id: id,
        part: part || Parititioner.part(id),
        data: data
    };
}

var EntityStore = Class({
    constructor: function (engine) {
        this.engine = engine;
    },

    create: function (type, id, data, done) {
        var entity = makeEntity(id, data);
        this.engine.insert(type, entity, function (err) {
            done(err, err ? undefined : entity);
        });
        return this;
    },

    update: function (type, id, data, done) {
        var entity = makeEntity(id, data);
        this.engine.update(type, entity, function (err) {
            done(err);
        });
        return this;
    },

    select: function (type, method, keys, done) {
        switch (method) {
            case 'id':
                this.engine.fetch(type, keys, function (err, results) {
                    done(err, err ? undefined : results.map(function (obj) {
                        return makeEntity(obj.id, obj.data, obj.part);
                    }));
                });
                break;
            case 'part':
                this.engine.queryPartitions(type, parseInt(keys[0]), parseInt(keys[1]), function (err, results) {
                    done(err, err ? undefined : results.map(function (obj) {
                        return obj.id;
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
