const React = require('react');
const moment = require('moment');

const LocationCalendarContainer = require('./LocationCalendarContainer');
const LocationMapContainer = require('./LocationMapContainer');
const TransformChooser = require('./TransformChooser');

let LocationPage = React.createClass({
  getInitialState: function () {
    return {
      transform: null,
      day: moment()
    };
  },

  getDefaultProps: function () {
    return {
      dataset: window.Viz.dataset,
      accessToken: window.Viz.accessToken
    };
  },

  _handleCalendarChange: function (currentDate) {
    this.setState({day: currentDate});
  },

  _handleTransformChange: function (type, param) {
    var transform = type ? {type: type, param: param} : null;
    this.setState({transform: transform});
  },

  render: function () {
    return (
      <div className="container-fluid">
        <div className="row">
          <div className="col-sm-2">
            <LocationCalendarContainer
              {...this.props}
              onChange={this._handleCalendarChange}/>
            <hr/>
            <TransformChooser onChange={this._handleTransformChange}/>
          </div>
          <div className="col-sm-10">
            <LocationMapContainer
              {...this.props}
              day={this.state.day}
              transform={this.state.transform}/>
          </div>
        </div>
      </div>
    );
  }
});

LocationPage.propTypes = {
  dataset: React.PropTypes.string.isRequired,
  accessToken: React.PropTypes.string.isRequired
};

module.exports = LocationPage;