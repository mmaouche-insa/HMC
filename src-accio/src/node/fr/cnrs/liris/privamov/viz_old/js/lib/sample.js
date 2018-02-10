var _ = require('lodash');

function sampleN(values, n) {
  var size = values.length;
  if (size <= n) {
    return values;
  } else {
    var ratio = size / n;
    return _.filter(values, function (v, idx) {
      return (idx % ratio) < 1
    });
  }
}

module.exports = {
  sampleN: sampleN
};