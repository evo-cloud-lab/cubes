var PARTITIONS = 4096;

function partition(key) {
    var sum = 0;
    for (var i = 0; i < key.length; i ++) {
        sum += key.charCodeAt(i);
    }
    return sum & (PARTITIONS - 1);
}

module.exports = {
    PARTITIONS: PARTITIONS,
    part: partition
};
