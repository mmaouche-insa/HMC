const React = require('react');
const _ = require('lodash');
const d3 = require('d3');

const MapGL = require('../../lib/react-map-gl/MapGL');
const ChoroplethOverlay = require('../../lib/react-map-gl/ChoroplethOverlay');
const PopupOverlay = require('../../lib/react-map-gl/PopupOverlay');

const MetricMapLegend = require('./MetricMapLegend');

//Use https://gka.github.io/palettes/ to generate a nice readable range.
const colorRange = ['#fffff0', '#ffe5ac', '#ffc86c', '#ffa918', '#fe8346', '#f26057', '#df4153', '#c72543', '#ab0b28', '#8b0000'];

function computeScale(data) {
  if (data.features.length === 0) {
    return [];
  }
  const domain = data.features.map(feature => feature.properties.value || 0);
  const colorScale = d3.scale
    .quantile()
    .domain(domain)
    .range(colorRange);
  const quantiles = colorScale.quantiles();
  return quantiles.map((quantile, idx) => {
    const color = colorScale(quantile);
    if (idx === 0) {
      return {from: 0, until: quantile, color: color};
    } else if (idx === quantiles.length - 1) {
      return {from: quantile, until: _.max(domain) + 1, color: color};
    } else {
      return {from: quantiles[idx - 1], until: quantile, color: color};
    }
  });
}

let MetricMap = React.createClass({
  getDefaultProps: function () {
    return {
      onViewportChange: _.noop,
      onTimeChange: _.noop,
      query: {}
    };
  },

  getInitialState: function () {
    return {
      viewport: {
        latitude: this.props.latitude,
        longitude: this.props.longitude,
        zoom: this.props.zoom
      },
      overlay: {
        display: false,
        latitude: 0,
        longitude: 0,
        properties: {id: null, value: null}
      }
    };
  },

  _handleViewportChange: function (viewport) {
    this.setState({viewport: viewport}, () => this.props.onViewportChange(this.state.viewport));
  },

  _handleTimeChange: function (time) {
    this.props.onTimeChange(time);
  },

  _handleClick: function (lngLat, features) {
    features = features.filter(feature => ('value' in feature.properties));
    if (features.length > 0) {
      const overlay = {
        display: true,
        longitude: lngLat.lng,
        latitude: lngLat.lat,
        properties: features[0].properties || {}
      };
      this.setState({overlay: overlay});
    } else if (this.state.overlay.display) {
      const overlay = _.merge({}, this.state.overlay, {display: false});
      this.setState({overlay: overlay});
    }
  },

  componentWillReceiveProps: function (nextProps) {
    const viewport = {
      latitude: nextProps.latitude,
      longitude: nextProps.longitude,
      zoom: nextProps.zoom
    };
    if (!_.isEqual(this.state.viewport, viewport)) {
      this.setState({viewport: viewport});
    }
  },

  render: function () {
    const scale = computeScale(this.props.data);
    return (
      <div>
        <div style={{position: 'relative'}}>
          <MapGL
            {...this.state.viewport}
            height={600}
            onClick={this._handleClick}
            onViewportChange={this._handleViewportChange}>
            <ChoroplethOverlay data={this.props.data} scale={scale} id="heatmap"/>
            <PopupOverlay display={this.state.overlay.display}
                          latitude={this.state.overlay.latitude}
                          longitude={this.state.overlay.longitude}>
              <b>Cell ID:</b> {this.state.overlay.properties.id}<br/>
              <b>Number of events:</b> {this.state.overlay.properties.value}
            </PopupOverlay>
          </MapGL>
          <div style={{position: 'absolute', bottom: '10px', right: '10px'}}>
            <MetricMapLegend scale={scale}
                             startAfter={this.props.query.start_after}
                             endBefore={this.props.query.end_before}/>
          </div>
        </div>
      </div>
    );
  }
});

MetricMap.propTypes = {
  data: React.PropTypes.object.isRequired,
  latitude: React.PropTypes.number.isRequired,
  longitude: React.PropTypes.number.isRequired,
  query: React.PropTypes.object,
  zoom: React.PropTypes.number.isRequired,
  onViewportChange: React.PropTypes.func.isRequired,
  onTimeChange: React.PropTypes.func.isRequired
};

module.exports = MetricMap;