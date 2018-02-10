const React = require('react');
const _ = require('lodash');
const $ = require('jquery');
const moment = require('moment');
const DateTime = require('react-datetime');

let LocationCalendarContainer = React.createClass({
  getInitialState: function () {
    return {
      activeDays: [],
      day: moment()
    };
  },

  getDefaultProps: function () {
    return {
      onChange: _.noop()
    };
  },

  _loadActiveDays: function () {
    this._loadActiveDaysRequest = $.ajax({
      url: window.Viz.apiEndpoint + '/datasets/' + this.props.dataset + '/sources',
      data: {access_token: this.props.accessToken},
      success: (json) => {
        this._loadActiveDaysRequest = $.ajax({
          url: window.Viz.apiEndpoint + '/datasets/' + this.props.dataset + '/sources/' + json[0],
          data: {access_token: this.props.accessToken},
          success: (json) => {
            var activeDays = _.map(json.active_days, (v) => moment(v));
            var day = _.last(activeDays);
            this.setState({activeDays: activeDays, day: day});
            this.props.onChange(day);
          },
          dataType: 'json'
        });
      },
      dataType: 'json'
    });
  },

  _isValidDate: function (currentDate) {
    return _.some(this.state.activeDays, function (v) {
      return currentDate.isSame(v, 'day');
    });
  },

  _handleChange: function (currentDate) {
    this.setState(_.merge(this.state, {day: currentDate}), function () {
      this.props.onChange(currentDate);
    }.bind(this));
  },

  componentDidMount: function () {
    this._loadActiveDays();
  },

  componentWillUnmount: function () {
    if (this._loadActiveDaysRequest) {
      this._loadActiveDaysRequest.abort();
    }
  },

  render: function () {
    return (
      <DateTime value={this.state.day}
                isValidDate={this._isValidDate}
                onChange={this._handleChange}
                timeFormat={false}/>
    );
  }
});

LocationCalendarContainer.propTypes = {
  dataset: React.PropTypes.string.isRequired,
  accessToken: React.PropTypes.string.isRequired,
  onChange: React.PropTypes.func
};

module.exports = LocationCalendarContainer;