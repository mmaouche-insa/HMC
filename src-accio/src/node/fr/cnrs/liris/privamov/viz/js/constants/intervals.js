const intervals = {
  'none': {label: 'None', duration: 0},
  '1h': {label: '1 hour', duration: 3600 * 1000},
  '2h': {label: '2 hours', duration: 2 * 3600 * 1000},
  '6h': {label: '6 hours', duration: 6 * 3600 * 1000},
  '12h': {label: '12 hours', duration: 12 * 3600 * 1000},
  '1d': {label: '1 day', duration: 24 * 3600 * 1000},
  '7d': {label: '7 days', duration: 7 * 24 * 3600 * 1000}
};

module.exports = intervals;