var Class = require('js-class'),

    Operation = require('./Operation');

var BlobRequestOp = Class(Operation, {
    constructor: function () {
        Operation.prototype.constructor.apply(this, arguments);
    },

    run: function () {
        // TODO
    },

    cancel: function () {
        // TODO
    }
});

module.exports = BlobRequestOp;
