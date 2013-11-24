var Class    = require('js-class'),
    elements = require('evo-elements'),
    Errors = elements.Errors,
    neuron   = require('evo-neuron'),

    EntityStore = require('./EntityStore'),
    BlobStore   = require('./BlobStore');

var BLOB_UPLOAD_SCHEMA = {
    path: 'string',
    metadata: { nullable: 'object' }
};

var BLOB_ID_SCHEMA = {
    id: 'string'
};

var BLOB_IDS_SCHEMA = {
    ids: { array: 'string' }
};

var OP_ABORT_SCHEMA = {
    opId: 'string'
};

var ENTITY_SCHEMA = {
    type: 'string',     // entity type
    id:   'string',     // unique identifier
    data: { nullable: 'object' }
};

var ENTITY_REFS_SCHEMA = {
    type: 'string',
    ids: { array: 'string' }
};

var ENTITY_SELECT_SCHEMA = {
    type: 'string',
    method: ['id', 'part'],
    keys: 'array'
};

var ENGINES = {
    mongodb: 'EntityEngineMongoDb',
    filesystem: 'BlobEngineFileSystem'
};

var ENGINE_DEFAULTS = {
    entity: {
        name: 'mongodb',
        uri: 'mongodb://localhost/cubes',
        options: {}
    },
    blob: {
        name: 'filesystem',
        path: '/tmp/blobs'
    }
};

var Program = Class(neuron.Program, {
    constructor: function () {
        neuron.Program.prototype.constructor.call(this, 'cubes');

        var engines = this.options.engines || {};
        ['entity', 'blob'].forEach(function (name) {
            var engineConf = engines[name] || ENGINE_DEFAULTS[name];
            this[name + 'Engine'] = require('./' + engineConf.name)(engineConf);
        }, this);
        this.entityStore = new EntityStore(this.entityEngine);
        (this.blobStore = new BlobStore(this.blobEngine, this.options))
            .on('progress', this.onOpProgress.bind(this))
            .on('complete', this.onOpComplete.bind(this))
        ;

        this
            .dispatch('blob.upload',   { schema: BLOB_UPLOAD_SCHEMA })
            .dispatch('blob.info',     { schema: BLOB_IDS_SCHEMA })
            .dispatch('blob.request',  { schema: BLOB_ID_SCHEMA })
            .dispatch('blob.release',  { schema: BLOB_ID_SCHEMA })
            .dispatch('blob.cleanup')
            .dispatch('op.list')
            .dispatch('op.abort',      { schema: OP_ABORT_SCHEMA })
            .dispatch('entity.create', { schema: ENTITY_SCHEMA })
            .dispatch('entity.update', { schema: ENTITY_SCHEMA })
            .dispatch('entity.select', { schema: ENTITY_SELECT_SCHEMA })
            .dispatch('entity.remove', { schema: ENTITY_REFS_SCHEMA })
        ;
    },

    'neuron:blob.upload': function (req, params) {
        this.blobStore.upload(req.src, params.path, req.done);
    },

    'neuron:blob.info': function (req, params) {
        this.blobStore.info(params.ids, function (err, info) {
            req.ok(err || { info: info });
        });
    },

    'neuron:blob.request': function (req, params) {
        this.blobStore.request(req.src, params.id, req.done);
    },

    'neuron:blob.release': function (req, params) {
        this.blobStore.release(req.src, params.id, req.done);
    },

    'neuron:blob.cleanup': function (req) {
        this.blobStore.cleanup(parseInt(req.data.delay));
        req.ok();
    },

    'neuron:op.list': function (req) {
        this.blobStore.listOps(req.src, function (err, ops) {
            req.ok(err || ops);
        });
    },

    'neuron:op.abort': function (req, params) {
        var op = this.ops.find(params.opId, req.src);
        if (op) {
            req.ok();
            op.abort();
        } else {
            req.fail(Errors.nonexist(params.opId));
        }
    },

    'neuron:entity.create': function (req, params) {
        this.entityStore.create(params.type, params.id, params.data, function (err, entity) {
            req.ok(err || { id: entity.id, part: entity.part });
        });
    },

    'neuron:entity.update': function (req, params) {
        this.entityStore.update(params.type, params.id, params.data, req.done);
    },

    'neuron:entity.select': function (req, params) {
        this.entityStore.select(params.type, params.method, params.keys, req.done);
    },

    'neuron:entity.remove': function (req, params) {
        this.entityStore.remove(params.type, params.ids, req.done);
    },

    onDisconnect: function (id) {
        this.blobStore.disconnect(id);
    },

    onOpProgress: function (opId, percentage, clients) {
        this.neuron.cast({
            event: 'op.progress',
            data: {
                opId: opId,
                progress: percentage
            }
        }, { target: clients });
    },

    onOpComplete: function (opId, err, clients) {
        var errData = err ? neuron.Message.err(err).data : undefined;
        this.neuron.cast({
            event: 'op.complete',
            data: {
                opId: opId,
                success: !err,
                error: errData
            }
        }, { target: clients });
    }
}, {
    statics: {
        run: function () {
            new Program().run();
        }
    }
});

module.exports = Program;
