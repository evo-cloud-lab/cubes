var Class = require('js-class'),
    Errors = require('evo-elements').Errors;

var _idBase = 0;

var Operation = Class(process.EventEmitter, {
    constructor: function (params, clientId, engine) {
        this.params = params;
        this._id = ++ _idBase;
        this._clientId = clientId;
        this._engine = engine;
        this._status = { state: 'new', data: {} };
    },

    get id () {
        return this._id;
    },

    get clientId () {
        return this._clientId;
    },

    get engine () {
        return this._engine;
    },

    get state () {
        return this._status.state;
    },

    get status () {
        return this._status;
    },

    updateStatus: function (data) {
        this._status.data = data;
        this._notify();
    },

    complete: function (err, result, cancelled) {
        this._status.state = 'completed';
        this._status.error = err;
        this._status.result = result;
        cancelled && (this._status.cancelled = true);
        this._notify();
        this.emit('complete', err, result, this);
    },

    start: function () {
        this._status.state = 'running';
        this._notify();
        this.run();
    },

    abort: function () {
        if (this.state == 'running') {
            this.cancel();
        }
    },

    // overridable methods
    run: function () {
        process.nextTick(function () {
            this.complete();
        }.bind(this));
    },

    cancel: function () {
        throw Errors.noSupport('cancel');
    },

    // privates

    _notify: function () {
        this.emit('status', this.status, this);
    }
});

module.exports = Operation;
