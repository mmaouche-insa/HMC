const React = require('react');
const $ = require('jquery');

const LocationMap = require('./LocationMap');
const LocationSeries = require('./TemporalGraph');

let LocationMapContainer = React.createClass({
  getInitialState: function () {
    return {data: []};
  },

  _loadGeoJson: function (props) {
    if (!props.day) {
      return;
    }
    let data = {
      access_token: props.accessToken,
      start_after: props.day.clone().startOf('day').format(),
      end_before: props.day.clone().endOf('day').format(),
      sample: true
    };
    if (props) {
      data.transform = props.type + '=' + props.param;
    }
    this._loadGeoJsonRequest = $.ajax({
      url: window.Viz.apiEndpoint + '/datasets/' + props.dataset + '/features',
      data: data,
      success: (json) => {
        this.setState({data: json});
      },
      dataType: 'json'
    });
  },

  componentWillReceiveProps: function (nextProps) {
    if (this._loadGeoJsonRequest) {
      this._loadGeoJsonRequest.abort();
    }
    this._loadGeoJson(nextProps);
  },

  componentDidMount: function () {
    this._loadGeoJson(this.props);
  },

  componentWillUnmount: function () {
    if (this._loadGeoJsonRequest) {
      this._loadGeoJsonRequest.abort();
    }
  },

  render: function () {
    return (
      <div>
        <LocationMap data={this.state.data}/>
        <LocationSeries data={this.state.data}/>
      </div>
    );
  }
});

LocationMapContainer.propTypes = {
  dataset: React.PropTypes.string.isRequired,
  accessToken: React.PropTypes.string.isRequired,
  day: React.PropTypes.object,
  transform: React.PropTypes.object
};

module.exports = LocationMapContainer;