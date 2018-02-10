const React = require('react');
const _ = require('lodash');
const d3 = require('d3');

function diffMap(prevMap, nextMap) {
  const prevKeys = _.keys(prevMap);
  const nextKeys = _.keys(nextMap);
  let enter = [];
  let update = [];
  let exit = [];
  prevKeys.forEach(key => {
    const next = nextMap[key];
    if (next) {
      if (!_.isEqual(next, prevMap[key])) {
        update.push({key: key, value: next});
      }
    } else {
      exit.push({key: key, value: prevMap[key]});
    }
  });
  nextKeys.forEach(key => {
    if (!(key in prevMap)) {
      enter.push({key: key, value: nextMap[key]});
    }
  });
  return {enter: enter, update: update, exit: exit};
}

function applySources(prevSources, nextSources, map) {
  const sourcesDiff = diffMap(prevSources, nextSources);
  sourcesDiff.exit.forEach(entry => map.removeSource(entry.key));
  sourcesDiff.update.forEach(entry => {
    map.removeSource(entry.key);
    map.addSource(entry.key, entry.value);
  });
  sourcesDiff.enter.forEach(entry => map.addSource(entry.key, entry.value));
}

function applyLayers(prevLayers, nextLayers, map) {
  const layersDiff = diffMap(prevLayers, nextLayers);
  layersDiff.exit.forEach(entry => map.removeLayer(entry.key));
  layersDiff.update.forEach(entry => {
    map.removeLayer(entry.key);
    map.addLayer(entry.value);
  });
  layersDiff.enter.forEach(entry => map.addLayer(entry.value));
}

function indexSources(sources) {
  const indexed = _.keyBy(sources, source => source.id);
  _.values(sources).forEach(source => delete source.id);
  return indexed;
}

function indexLayers(layers) {
  return _.keyBy(layers, layer => layer.id);
}

let ChoroplethOverlay = React.createClass({
  getDefaultProps: function () {
    return {
      opacity: 0.8,
      valueProperty: 'value',
      map: null
    }
  },

  getInitialState: function () {
    return {
      sources: {},
      layers: {}
    }
  },

  _getSources: function (props) {
    return [
      {
        id: props.id,
        type: 'geojson',
        data: props.data
      }
    ];
  },

  _getLayers: function (props) {
    if (props.data.features.length === 0) {
      return [];
    }

    const fillLayers = this.props.scale.map((range, idx) => {
      return {
        id: props.id + '-' + idx,
        type: 'fill',
        source: props.id,
        paint: {
          'fill-color': range.color,
          'fill-opacity': props.opacity
        },
        filter: [
          'all',
          ['>=', this.props.valueProperty, range.from],
          ['<', this.props.valueProperty, range.until]
        ]
      };
    });
    const lineLayer = {
      id: props.id + '-line',
      type: 'line',
      source: props.id,
      paint: {
        'line-color': '#ffffff',
        'line-width': 0.5
      }
    };
    return fillLayers.concat([lineLayer]);
  },

  _update: function (props) {
    if (!props.map) {
      return;
    }
    const sources = indexSources(this._getSources(props));
    const layers = indexLayers(this._getLayers(props));
    applySources(this.state.sources, sources, props.map);
    applyLayers(this.state.layers, layers, props.map);

    this.setState({sources: sources, layers: layers});
  },

  componentDidMount: function () {
    this._update(this.props);
  },

  componentWillReceiveProps: function (nextProps) {
    this._update(nextProps);
  },

  render: function () {
    return null;
  }
});

ChoroplethOverlay.propTypes = {
  id: React.PropTypes.string.isRequired,
  data: React.PropTypes.object.isRequired,
  scale: React.PropTypes.arrayOf(React.PropTypes.object).isRequired,
  valueProperty: React.PropTypes.string.isRequired,
  opacity: React.PropTypes.number,
  map: React.PropTypes.object
};

module.exports = ChoroplethOverlay;