var Class  = require('js-class'),
    path   = require('path');
    Config = require('evo-elements').Config;

var SERVICE = 'cubes';

var cli;

function request(neuron, msg, next) {
    cli.logAction('Request');
    cli.logObject(msg);
    neuron.request(SERVICE, msg, function (err, resp) {
        err && cli.fatal(err);
        cli.logAction('Response');
        cli.logObject(resp);
        next ? next(resp, neuron) : process.exit(0);
    });
}

function execute(event, data, opts, next) {
    cli.neuronConnectService(SERVICE, opts, function (neuron) {
        request(neuron, { event: event, data: data }, next);
    });
}

var COMPLETE_STYLUS = {
    state: function (state) {
        return { SUCCESS: cli.ok, FAILURE: cli.err }[state].call(cli, state);
    }
};

function waitOp(neuron) {
    neuron
        .subscribe('op.progress', SERVICE, function (msg) {
            cli.logAction('Progress', null, msg.data.progress);
            cli.logObject(msg);
        })
        .subscribe('op.complete', SERVICE, function (msg) {
            cli.logAction('Complete', null, msg.data.success ? 'SUCCESS' : 'FAILURE', COMPLETE_STYLUS);
            cli.logObject(msg);
            !msg.data.success && msg.data.error && cli.fatal(msg.data.error);
            cli.exit();
        });
}

function blobUpload(opts) {
    execute('blob.upload', { path: path.resolve(process.cwd(), opts.FILE) }, opts, function (resp, neuron) {
        waitOp(neuron);
    });
}

function blobDownload(opts) {
    execute('blob.request', { id: opts.BLOBID }, opts, function (resp, neuron) {
        if (resp.data.ready) {
            cli.exit();
        } else {
            waitOp(neuron);
        }
    });
}

function blobInfo(opts) {
    execute('blob.info', { ids: opts.BLOBID }, opts);
}

function blobRemove(opts) {
    cli.fatal('Not implemented');
}

function blobClean(opts) {
    execute('blob.cleanup', { delay: opts.delay }, opts);
}

function loadEntityData(data) {
    var cfg = new Config();
    cfg.parse(['--data=' + data]);
    return cfg.opts.data;
}

function entityCreate(opts) {
    var data = loadEntityData(opts.DATA);
    execute('entity.create', { type: opts.TYPE, id: opts.ID, data: data }, opts);
}

function entityUpdate(opts) {
    var data = loadEntityData(opts.DATA);
    execute('entity.update', { type: opts.TYPE, id: opts.ID, data: data }, opts);
}

function entityGet(opts) {
    execute('entity.select', { type: opts.TYPE, method: 'id', keys: opts.ID }, opts);
}

function entityList(opts) {
    execute('entity.select', { type: opts.TYPE, method: 'part', keys: [opts.PART, opts.COUNT] }, opts);
}

function entityRemove(opts) {
    execute('entity.remove', { type: opts.TYPE, ids: opts.ID }, opts);
}

module.exports = function (theCli) {
    cli = theCli;

    cli.neuronCmd('blob:upload', function (cmd) {
        cmd.help('Upload a blob from local file')
            .option('FILE', {
                position: 1,
                required: true,
                type: 'string',
                help: 'Local file name'
            });
    }, blobUpload);

    cli.neuronCmd('blob:download', function (cmd) {
        cmd.help('Download blob into cache')
            .option('BLOBID', {
                position: 1,
                required: true,
                type: 'string',
                help: 'Blob Id'
            });
    }, blobDownload);

    cli.neuronCmd('blob:info', function (cmd) {
        cmd.help('Display blob information')
            .option('BLOBID', {
                position: 1,
                required: true,
                list: true,
                type: 'string',
                help: 'Blob Id'
            });
    }, blobInfo);

    cli.neuronCmd('blob:remove', function (cmd) {
        cmd.help('Delete blobs')
            .option('BLOBID', {
                position: 1,
                required: true,
                type: 'string',
                list: true,
                help: 'Blob Ids'
            });
    }, blobRemove);

    cli.neuronCmd('blob:clean', function (cmd) {
        cmd.help('Clean blob cache')
            .option('delay', {
                metavar: 'MS',
                required: false,
                type: 'integer',
                help: 'Delay cleaning in milliseconds'
            });
    }, blobClean);

    cli.neuronCmd('entity:create', function (cmd) {
        cmd.help('Create an entity')
            .option('TYPE', {
                position: 1,
                required: true,
                type: 'string',
                help: 'Entity type'
            })
            .option('ID', {
                position: 2,
                required: true,
                type: 'string',
                help: 'Entity Id'
            })
            .option('DATA', {
                position: 3,
                required: true,
                type: 'string',
                help: 'Entity data in JSON or in file (JSON/YAML) (prefixed with @)'
            });
    }, entityCreate);

    cli.neuronCmd('entity:update', function (cmd) {
        cmd.help('Update an entity')
            .option('TYPE', {
                position: 1,
                required: true,
                type: 'string',
                help: 'Entity type'
            })
            .option('ID', {
                position: 2,
                required: true,
                type: 'string',
                help: 'Entity Id'
            })
            .option('DATA', {
                position: 3,
                required: true,
                type: 'string',
                help: 'Entity data in JSON or in file (JSON/YAML) (prefixed with @)'
            });
    }, entityUpdate);

    cli.neuronCmd('entity:get', function (cmd) {
        cmd.help('Query entities by Ids')
            .option('TYPE', {
                position: 1,
                required: true,
                type: 'string',
                help: 'Entity type'
            })
            .option('ID', {
                position: 2,
                required: true,
                type: 'string',
                list: true,
                help: 'Entity Ids'
            });
    }, entityGet);

    cli.neuronCmd('entity:list', function (cmd) {
        cmd.help('Query entity Ids in partitions')
            .option('TYPE', {
                position: 1,
                required: true,
                type: 'string',
                help: 'Entity type'
            })
            .option('PART', {
                position: 2,
                required: true,
                type: 'integer',
                help: 'Start partition'
            })
            .option('COUNT', {
                position: 3,
                required: false,
                default: 1,
                type: 'integer',
                help: 'Partitions count'
            });
    }, entityList);

    cli.neuronCmd('entity:remove', function (cmd) {
        cmd.help('Delete entities by Ids')
            .option('TYPE', {
                position: 1,
                required: true,
                type: 'string',
                help: 'Entity type'
            })
            .option('ID', {
                position: 2,
                required: true,
                type: 'string',
                list: true,
                help: 'Ids of entities to delete'
            });
    }, entityRemove);
};
