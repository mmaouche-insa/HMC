const React = require('react');
const ReactDOM = require('react-dom');
const d3 = require('d3');
const MapboxGL = require('mapbox-gl');


let PopupOverlay = React.createClass({
  _update: function (prevProps, nextProps) {
    if (!nextProps.map) {
      return;
    }
    if (nextProps.display !== prevProps.display) {
      if (nextProps.display) {
        this._popup.addTo(nextProps.map);
      } else if (prevProps.display) {
        this._popup.remove();
      }
    }
    if (nextProps.display && nextProps.latitude && nextProps.longitude) {
      if (nextProps.latitude === null || nextProps.longitude === null) {
        throw new Error('Unable to render PopupOverlay with {latitude:' + nextProps.latitude + ',longitude:' + nextProps.longitude + '}');
      } else {
        if (nextProps.latitude !== prevProps.latitude || nextProps.longitude !== prevProps.longitude) {
          this._popup.setLngLat(new MapboxGL.LngLat(nextProps.longitude, nextProps.latitude));
        }
        const inner = document.createElement('div');
        ReactDOM.render(<div>{nextProps.children}</div>, inner);
        this._popup.setHTML(inner.children[0].innerHTML);
      }
    }
  },

  componentWillReceiveProps: function (nextProps) {
    this._update(this.props, nextProps);
  },

  componentDidMount: function () {
    this._popup = new MapboxGL.Popup({
      closeButton: false,
      closeOnClick: false
    });
    //At this point `this.props.map` is normally not set, so there is no point on going further.
  },

  componentWillUnmount: function () {
    if (this._popup && this.props.display) {
      this._popup.remove();
    }
  },

  render: function () {
    return null;
  }
});

PopupOverlay.propTypes = {
  display: React.PropTypes.bool.isRequired,
  latitude: React.PropTypes.number,
  longitude: React.PropTypes.number,
  map: React.PropTypes.object
};

module.exports = PopupOverlay;