var Class    = require('js-class'),
    elements = require('evo-elements'),
    Errors = elements.Errors,
    neuron   = require('evo-neuron'),

    BlobUploadOp  = require('./BlobUploadOp'),
    BlobRequestOp = require('./BlobRequestOp'),
    BlobReleaseOp = require('./BlobReleaseOp'),
    OpTracker = require('./OpTracker');

var SCHEMAS = {
    'blob.upload': {
        path: 'string',
        metadata: { nullable: 'object' }
    },
    'blob.request': {
        blobId: 'string',
        ownerId: 'string'
    },
    'blob.release': {
        blobId: 'string',
        ownerId: 'string'
    }
};

var ABORT_SCHEMA = {
    opId: 'string'
};

var Program = Class(neuron.Program, {
    constructor: function () {
        neuron.Program.prototype.constructor.call(this, 'cubes');

        var engineName = this.options.engine || 'filesystem';
        this.engine = require('./engines/' + engineName)(this.options);
        this.ops = new OpTracker(this.neuron);

        this
            ._dispatch('blob.upload',  BlobUploadOp)
            ._dispatch('blob.request', BlobRequestOp)
            ._dispatch('blob.release', BlobReleaseOp)
            .dispatch('abort', { schema: ABORT_SCHEMA })
        ;
    },

    _dispatch: function (name, opClass) {
        this.dispatch(name, { schema: SCHEMAS[name] });
        this['neuron:' + name] = function (req, params) {
            var op = new opClass(params, req.src, this.engine);
            this.ops.add(op);
            req.ok({ opId: op.id });
            op.start();
        }.bind(this);
        return this;
    },

    'neuron:abort': function (req, params) {
        var op = this.ops.find(params.opId, req.src);
        if (op) {
            req.ok();
            op.abort();
        } else {
            req.fail(Errors.nonexist(params.opId));
        }
    }
}, {
    statics: {
        run: function () {
            new Program().run();
        }
    }
});

module.exports = Program;
