const React = require('react');
const _ = require('lodash');
const MetricMap = require('./MetricMap');

let MetricMapContainer = React.createClass({
  getInitialState: function () {
    return {
      data: {
        type: 'FeatureCollection',
        features: []
      },
      viewport: {
        latitude: 45.76,
        longitude: 4.84,
        zoom: 8
      },
      scale: null
    };
  },

  _loadData: function (props) {
    //TODO: validate props.query is valid.
    const q = {
      filters: [
        {
          type: 'time_range',
          from: props.query.start_after.format(),
          to: props.query.end_before.format()
        }
      ],
      aggregate: {
        type: 'heatmap',
        level: props.query.resolution
      }
    };
    fetch(Viz.apiEndpoint + '/datasets/' + props.query.dataset + '/events?format=geojson', {
      body: JSON.stringify(q),
      method: 'POST',
      headers: {'Content-Type': 'application/json; charset=UTF-8'}
    }).then(resp => resp.json())
      .then(data => {
        //TODO: update viewort accordingly.
        const changes = {data: data};
        this.setState(changes);
      });
  },

  _computeViewport: function (data) {
    if (data.bbox) {
      return {
        latitude: (data.bbox[1] + data.bbox[3]) / 2,
        longitude: (data.bbox[0] + data.bbox[2]) / 2,
        zoom: this.state.viewport.zoom
      };
    } else {
      return this.state.viewport;
    }
  },

  _handleTimeChange: function (time) {
    this._loadData(this.props, time);
  },

  _handleViewportChange: function (viewport) {
    this.setState({viewport: viewport});
  },

  componentDidMount: function () {
    if (this.props.query) {
      this._loadData(this.props);
    }
  },

  componentWillReceiveProps: function (nextProps) {
    if (!_.isEqual(this.props, nextProps)) {
      if (nextProps.query) {
        this._loadData(nextProps);
      } else {
        this.setState(this.getInitialState());
      }
    }
  },

  render: function () {
    return (
      <MetricMap
        {...this.state.viewport}
        data={this.state.data}
        scale={this.state.scale}
        query={this.props.query}
        onTimeChange={this._handleTimeChange}
        onViewportChange={this._handleViewportChange}/>
    );
  }
});

MetricMapContainer.propTypes = {
  query: React.PropTypes.object
};

module.exports = MetricMapContainer;