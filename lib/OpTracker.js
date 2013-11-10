var Class    = require('js-class'),
    elements = require('evo-elements'),
    BiMap  = elements.BiMap;

var OpTracker = Class({
    constructor: function (neuron) {
        this._ops = new BiMap('op', 'client');

        neuron
            .on('disconnect', this.onDendriteDisconnect.bind(this))
        ;
    },

    add: function (op) {
        this._ops.add(op.id, op.clientId, op);
        op.on('complete', function () {
            this.remove(op);
        }.bind(this));
        return op;
    },

    remove: function (op) {
        this._ops.remove(op.id, op.clientId);
    },

    find: function (opId, clientId) {
        return this._ops.get(opId, clientId);
    },

    onDendriteDisconnect: function (id) {
        // TODO
    }
});

module.exports = OpTracker;
