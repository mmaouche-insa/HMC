const resolutions = _.range(0, 31).map(level => {
  return {key: level, label: 'Level ' + level}
});

module.exports = resolutions;