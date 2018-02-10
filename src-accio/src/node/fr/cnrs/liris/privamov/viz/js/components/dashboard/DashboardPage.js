const React = require('react');
const Grid = require('react-bootstrap/lib/Grid');
const Row = require('react-bootstrap/lib/Row');
const Col = require('react-bootstrap/lib/Col');

const QueryBuilderContainer = require('./QueryBuilderContainer');
const MetricMapContainer = require('./MetricMapContainer');

let DashboardPage = React.createClass({
  _handleQuerySubmit: function (query) {
    this.setState({query: query});
  },

  getInitialState: function () {
    return {
      query: undefined
    };
  },

  render: function () {
    return (
      <div>
        <QueryBuilderContainer onSubmit={this._handleQuerySubmit}/>
        <Grid fluid>
          <Row>
            <Col sm={12}>
              <MetricMapContainer query={this.state.query}/>
            </Col>
          </Row>
        </Grid>
      </div>
    );
  }
});

module.exports = DashboardPage;