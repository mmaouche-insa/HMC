const React = require('react');
const _ = require('lodash');
const L = require('leaflet');
const moment = require('moment');

let LocationMap = React.createClass({
  _updateMap: function (data) {
    if (data.features && data.features.length) {
      // We create a GeoJSON layer displaying all points.
      var points = L.geoJson(data, {
        pointToLayer: function (feature, latLng) {
          return L.circleMarker(latLng, {
            fillColor: '#FF6600',
            color: 'black',
            fillOpacity: 1,
            radius: 6
          });
        },
        onEachFeature: function (feature, layer) {
          if (feature.properties && feature.properties.time) {
            var t = moment(feature.properties.time);
            layer.bindPopup('<b>' + t.format('MMM Do YYYY HH:mm:ss') + '</b>');
          }
        }
      }).addTo(this._leafletMap);

      // Adapt the map bounds to these points.
      this._leafletMap.fitBounds(points);

      // We create a polyline joining all points.
      var line = _.map(data.features, function (feature) {
        return L.latLng(feature.geometry.coordinates[1], feature.geometry.coordinates[0]);
      });
      L.polyline(line, {color: '#FF5C00'}).addTo(this._leafletMap);
    } else {
      this._leafletMap.fitWorld();
    }
  },

  componentWillUpdate: function (nextProps, nextState) {
    // We remove all layers, except the base tile layer displaying the background.
    this._leafletMap.eachLayer(function (layer) {
      if (!(layer instanceof L.TileLayer)) {
        this._leafletMap.removeLayer(layer);
      }
    }.bind(this));
    this._updateMap(nextProps.data);
  },

  componentDidMount: function () {
    this._leafletMap = L.map(this.props.id);
    new L.TileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
      minZoom: 2,
      maxZoom: 19,
      attribution: 'Map data © <a href="https://openstreetmap.org">OpenStreetMap</a> contributors'
    }).addTo(this._leafletMap);
    this._updateMap(this.props.data);
  },

  getDefaultProps: function () {
    return {
      id: _.uniqueId('map')
    };
  },

  render: function () {
    return (
      <div id={this.props.id} className="location-map"></div>
    );
  }
});

module.exports = LocationMap;