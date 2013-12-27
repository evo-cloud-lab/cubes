var Class  = require('js-class'),
    async  = require('async'),
    idioms = require('evo-idioms'),
    elements = require('evo-elements'),
    BiMap  = elements.BiMap,
    Logger = elements.Logger,
    Utils  = elements.Utils,
    Catalog    = elements.Catalog,
    DelayedJob = elements.DelayedJob;

var STATE_KEY = 'kv.parts';

var Value = Class({
    constructor: function (key) {
        this.key = key;
        this._watchers = {};
    },

    get watchers () {
        return Object.keys(this._watchers);
    },

    watch: function (nodeId) {
        this._watchers[nodeId] = true;
    },

    unwatch: function (nodeId) {
        delete this._watchers[nodeId];
    }
});

var KeyValueStore = Class(Catalog, {
    constructor: function () {
        Catalog.prototype.constructor.call(this, []);
    },

    addValue: function (key, part) {
        var values = this.all(part, true);
        var value = values[key];
        value || (value = values[key] = new Value(key));
        return value;
    }
});

var Watcher = Class({
    constructor: function (key) {
        this.key = key;
        this.part = idiom.Partitioner.part(key);
    },

    notify: function (neuron, clients) {
        if (clients.length > 0) {
            neuron.cast({ event: 'kv.changed', data: { key: key, value: this.value } }, { target: clients });
        }
    }
});

var KeyWatcher = Class({
    constructor: function (connector, options, logger) {
        this.connector = connector;
        this.options = options;
        this.logger = Logger.clone(logger, { prefix: '<kv> ' });
        this.neuron = connector.neuron;

        this._requestTimeout = options.watcherRetry || 3000;

        this._watchers = new idioms.PartitionMapper(STATE_KEY, 'expect');
        this._watching = new BiMap('key', 'client');
        this._removedWatchers = new Catalog([]);
        this._watchRequestJob = new DelayedJob(this._requestWatchers.bind(this), this._requestTimeout);

        this._publishers = new idioms.PartitionMapper(STATE_KEY, 'expect');
        this._publishing = new BiMap('key', 'client');
        this._publishingJob = new DelayedJob(this._sendPublishes.bind(this));

        this._store = new KeyValueStore();

        this._partitioner = new idioms.Partitioner.ClusterPartitioner(STATE_KEY);

        this._states = new idioms.ConnectorStates(this.connector, {
            master: {
                update: this._masterUpdate
            },
            member: {
                update: this._memberUpdate
            },
            context: this
        });
        this._states.start();

        this.connector.on('message', this.onMessage.bind(this));
    },

    watch: function (keys, clientId, done) {
        var diff = Utils.diff(this._watching.keys(clientId, 'client'), keys);
        var fns = [this._clientUnwatch, this._clientWatch];
        for (var i = 0; i < 2; i ++) {
            diff[i].forEach(function (key) { fns[i].call(this, key, clientId); }, this);
        }
        done();
    },

    publish: function (key, value, options, clientId, done) {
        if (options.publisher) {
            this._clientPub(key, clientId, value, options.expireIn);
        } else {
            this._sendUpdate(key, value, options.expireIn);
        }
        done();
    },

    disconnect: function (clientId) {
        this._watching.keys(clientId, 'client')
            .forEach(function (key) {
                this._clientUnwatch(key, clientId);
            }, this);
        this._publishing.keys(clientId, 'client')
            .forEach(function (key) {
                this._clientUnpub(key, clientId);
            }, this);
    },

    onMessage: function (msg, src) {
        if (msg.event.substr(0, 9) == 'cubes.kv.') {
            var method = this['msg:' + msg.event.substr(9)];
            method && method.apply(this, arguments);
        }
    },

    'msg:watch': function (msg, src) {
        if (!this._part) {
            return;
        }
        Array.isArray(msg.data.unwatch) && msg.data.unwatch.forEach(function (key) {
            var part = idioms.Partitioner.part(key);
            var values = this._store.all(part);
            var value = values && values[key];
            value && value.unwatch(src);
        }, this);
        var changes = {};
        Array.isArray(msg.data.watch) && msg.data.watch.forEach(function (key) {
            var part = idioms.Partitioner.part(key);
            if (part >= this._part.begin && part < this._part.end) {
                var value = this._store.addValue(key, part);
                value.watch(src);
                changes[key] = value.value;
            }
        }, this);
        Object.keys(changes).length > 0 && this.connector.send({ event: 'cubes.kv.changed', data: changes }, src);
    },

    'msg:update': function (msg, src) {
        if (!this._part) {
            return;
        }

        var changes = [];
        for (var key in msg.data) {
            var part = idioms.Partitioner.part(key);
            if (part >= this._part.begin && part < this._part.end) {
                var value = this._store.addValue(key, part);
                value.value = msg.data[key].value;
                value.expireIn = msg.data[key].expireIn;
                changes.push(value);
            }
        }

        var nodes = new Catalog();
        changes.forEach(function (value) {
            value.watchers.forEach(function (id) {
                nodes.add(id, value.key, value.value);
            });
        });
        nodes.names.forEach(function (id) {
            this.connector.send({ event: 'cubes.kv.changed', data: nodes.all(id) }, id);
        }, this);
    },

    'msg:changed': function (msg, src) {
        var unwatched = [], watched = [];
        for (var key in msg.data) {
            var watcher = this._watchers.get(key);
            if (watcher) {
                watcher.value = msg.data[key];
                watched.push(key);
                watcher.notify(this.neuron, this._watching.keys(key, 'key'));
            } else {
                unwatched.push(key);
            }
        }
        this._watchers.markMapped(watched, true);
        if (unwatched.length > 0) {
            this.connector.send({ event: 'cubes.kv.watch', data: { unwatch: unwatched } }, src);
        }
    },

    // Watchers

    _clientWatch: function (key, clientId) {
        var watcher = this._watch(key);
        this._watching.add(key, clientId, watcher);
        if (watcher.value !== undefined) {
            watcher.notify(this.neuron, [clientId]);
        }
    },

    _clientUnwatch: function (key, clientId) {
        this._watching.remove(key, clientId);
        if (this._watching.all(key, 'key') == null) {
            this._unwatch(key);
        }
    },

    _watch: function (key) {
        var watcher = this._watchers.get(key);
        if (!watcher) {
            watcher = new Watcher(key);
            this._watchers.add(key, watcher);
            this._watchRequestJob.reschedule(0);
        }
        return watcher;
    },

    _unwatch: function (key) {
        var watcher = this._watchers.remove(key);
        if (watcher) {
            this._removedWatchers.add(watcher.part, key, watcher);
            this._watchRequestJob.reschedule(0);
        }
    },

    _requestWatchers: function () {
        var removals = {}, reschedule;
        this._removedWatchers.names.forEach(function (part) {
            var node = this._watchers.map.find(part);
            if (node) {
                var keys = removals[node.id];
                keys || (keys = removals[node.id] = {});
                _.extend(keys, this._removedWatchers.all(part));
            }
        }, this);
        this._removedWatchers.clear([]);

        var maps = this._watchers.unmappedByNodes();
        for (var nodeId in maps.nodes) {
            var msg = { event: 'cubes.kv.watch', data: { watch: Object.keys(maps.nodes[nodeId]) } };
            if (removals[nodeId]) {
                msg.data.unwatch = Object.keys(removals[nodeId]);
                delete removals[nodeId];
            }
            this.connector.send(msg, nodeId);
            reschedule = true;
        }
        for (var nodeId in removals) {
            var msg = { event: 'cubes.kv.watch', data: { unwatch: Object.keys(removals[nodeId]) } };
            this.connector.send(msg, nodeId);
            reschedule = true;
        }

        if (reschedule || Object.keys(maps.unmapped).length > 0) {
            this._watchRequestJob.schedule();
        }
    },

    // Publishers
    _clientPub: function (key, clientId, value, expireIn) {
        var publisher = this._publish(key, value, expireIn);
        this._publishing.add(key, clientId, publisher);
    },

    _clientUnpub: function (key, clientId) {
        this._publishing.remove(key, clientId);
        if (this._publishing.all(key, 'key') == null) {
            this._unpublish(key);
        }
    },

    _publish: function (key, value, expireIn) {
        var publisher = { value: value, expireIn: expireIn };
        this._publishers.add(key, publisher);
        this._publishingJob.reschedule(0);
        return publisher;
    },

    _unpublish: function (key) {
        this._publishers.remove(key);
    },

    _sendUpdate: function (key, value, expireIn) {
        var part = idioms.Partitioner.part(key);
        var node = this._publishers.map.find(part);
        if (node) {
            var data = {};
            data[key] = { value: value, expireIn: expireIn };
            this.connector.send({ event: 'cubes.kv.update', data: data }, node.id);
            return true;
        }
        return false;
    },

    _sendPublishes: function () {
        this._publishers.remap(function (nodeId, publishers, next) {
            var data = {};
            for (var key in publishers) {
                var pub = publishers[key];
                data[key] = { value: pub.value, expireIn: pub.expireIn };
            }
            this.connector.send({ event: 'cubes.kv.update', data: data }, nodeId);
            next(null, true);
        }.bind(this));
    },

    // Store
    _cleanupStore: function () {
        if (this._part) {
            this._store.names.forEach(function (part) {
                if (part < this._part.begin || part >= this._part.end) {
                    this._store.removeAll(part);
                }
            }, this);
        } else {
            this._store.clear([]);
        }
    },

    _clusterUpdated: function (clusterInfo) {
        // find expect partition for local node
        for (var n in clusterInfo.nodes) {
            var node = clusterInfo.nodes[n];
            if (node.id == clusterInfo.localId) {
                this._part = idioms.PartitionMap.partInfo(node, 'kv.parts', 'expect');
                break;
            }
            this._cleanupStore();
        }

        this._watchers.clusterUpdate(clusterInfo);
        this._publishers.clusterUpdate(clusterInfo);
        this._watchRequestJob.reschedule(0);
        this._publishingJob.reschedule(0);
    },

    _masterUpdate: function (clusterInfo) {
        var states = this._partitioner.partition(clusterInfo);
        states && connector.expects(states);
        this._clusterUpdated(clusterInfo);
    },

    _memberUpdate: function (clusterInfo) {
        this._clusterUpdated(clusterInfo);
    }
});

module.exports = KeyWatcher;
