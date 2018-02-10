const React = require('react');
const _ = require('lodash');
const moment = require('moment');
const QueryBuilder = require('./QueryBuilder');

let QueryBuilderContainer = React.createClass({
  getDefaultProps: function () {
    return {
      onSubmit: _.noop
    };
  },

  getInitialState: function () {
    return {
      datasets: []
    };
  },

  _loadDatasets: function () {
    fetch(Viz.apiEndpoint + '/datasets', {method: 'GET'})
      .then(resp => resp.json())
      .then(datasets => {
        datasets.forEach(dataset => {
          //Convert from/to dates into moment's.
          dataset.from = moment(dataset.from);
          dataset.to = moment(dataset.to);
          return dataset;
        });
        this.setState({datasets: datasets});
      });
  },

  componentDidMount: function () {
    this._loadDatasets();
  },

  render: function () {
    return <QueryBuilder
      datasets={this.state.datasets}
      onSubmit={this.props.onSubmit}/>;
  }
});

QueryBuilderContainer.propTypes = {
  onSubmit: React.PropTypes.func.isRequired
};

module.exports = QueryBuilderContainer;