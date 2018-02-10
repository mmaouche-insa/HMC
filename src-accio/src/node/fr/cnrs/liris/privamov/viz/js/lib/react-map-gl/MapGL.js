const React = require('react');
const _ = require('lodash');
const d3 = require('d3');
const MapboxGL = require('mapbox-gl');

const MAPBOX_ACCESS_TOKEN = 'pk.eyJ1IjoidnByaW1hdWx0IiwiYSI6IjdER2M5VHcifQ.szC24ecvnE5QdO61mBGt5Q';
MapboxGL.accessToken = MAPBOX_ACCESS_TOKEN;

let MapGL = React.createClass({
  _updateStateFromProps: function (props) {
    return {
      viewport: {
        latitude: props.latitude,
        longitude: props.longitude,
        zoom: props.zoom
      }
    };
  },

  _update: function (prevProps, nextProps) {
    if (prevProps.latitude !== nextProps.latitude
      || prevProps.longitude !== nextProps.longitude
      || prevProps.zoom !== nextProps.zoom
    ) {
      this._map.jumpTo({
        center: new MapboxGL.LngLat(nextProps.longitude, nextProps.latitude),
        zoom: nextProps.zoom
      });
    }
    if (prevProps.width !== nextProps.width || prevProps.height !== nextProps.height) {
      this._map.resize();
    }
  },

  _handleMove: function () {
    const center = this._map.getCenter();
    const zoom = this._map.getZoom();
    const changes = {
      viewport: {
        latitude: center.lat,
        longitude: center.lng,
        zoom: zoom
      }
    };
    this.setState(changes, () => this.props.onViewportChange(changes.viewport));
  },

  _handleClick: function (e) {
    const features = this._map.queryRenderedFeatures(e.point);
    this.props.onClick(e.lngLat, features);
  },

  getInitialState: function () {
    return {
      loaded: false
    };
  },

  componentWillReceiveProps: function (nextProps) {
    this._update(this.props, nextProps);
    this.setState(this._updateStateFromProps(nextProps));
  },

  componentWillMount: function () {
    this.setState(this._updateStateFromProps(this.props));
  },

  componentDidMount: function () {
    this._map = new MapboxGL.Map({
      container: this.refs.mapboxMap,
      style: 'mapbox://styles/mapbox/streets-v8',
      center: new MapboxGL.LngLat(this.props.longitude, this.props.latitude),
      zoom: this.props.zoom,
      attributionControl: false
    });
    d3.select(this._map.getCanvas()).style('outline', 'none');

    if (this.props.onClick) {
      this._map.on('click', this._handleClick);
    }
    if (this.props.onViewportChange) {
      this._map.on('moveend', this._handleMove);
    }
    this._map.on('load', () => this.setState({loaded: true}));
  },

  componentWillUnmount: function () {
    if (this._map) {
      this._map.remove();
    }
  },

  render: function () {
    const style = {width: this.props.width, height: this.props.height};
    const children = this.state.loaded ?
      React.Children.map(this.props.children, elem => React.cloneElement(elem, {map: this._map})) :
      this.props.children;
    return (
      <div ref="mapboxMap" style={style}>
        {children}
      </div>
    );
  }
});

MapGL.propTypes = {
  /**
   * The latitude of the center of the map.
   */
  latitude: React.PropTypes.number.isRequired,
  /**
   * The longitude of the center of the map.
   */
  longitude: React.PropTypes.number.isRequired,
  /**
   * The tile zoom level of the map.
   */
  zoom: React.PropTypes.number.isRequired,
  /**
   * The width of the map.
   */
  width: React.PropTypes.number,
  /**
   * The height of the map.
   */
  height: React.PropTypes.number.isRequired,
  /**
   * `onViewportChange` callback is fired when the user interacted with the
   * map. The object passed to the callback containers `latitude`,
   * `longitude` and `zoom` information.
   */
  onViewportChange: React.PropTypes.func,

  onClick: React.PropTypes.func
};

module.exports = MapGL;