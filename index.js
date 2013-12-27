module.exports = {
    EntityStore: require('./lib/EntityStore'),
    EntityEngineMongoDb: require('./lib/EntityEngineMongoDb'),
    BlobStore:   require('./lib/BlobStore'),
    BlobEngineFileSystem: require('./lib/BlobEngineFilesystem'),
    KeyWatcher:  require('./lib/KeyWatcher'),
    Program:     require('./lib/Program'),
    cli:         require('./cli')
};
