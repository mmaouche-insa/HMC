const React = require('react');
const Router = require('react-router').Router;
const Route = require('react-router').Route;
const IndexRoute = require('react-router').IndexRoute;
const hashHistory = require('react-router').hashHistory;

const AppController = require('./components/AppController');
const DashboardPage = require('./components/dashboard/DashboardPage');

const App = React.createClass({
  render: function () {
    return (
      <Router history={hashHistory}>
        <Route path="/" component={AppController}>
          <IndexRoute component={DashboardPage}/>
        </Route>
      </Router>
    );
  }
});

module.exports = App;