const React = require('react');
const Link = require('react-router').Link;

const App = React.createClass({
  render: function () {
    return (
      <div>
        <div className="navbar navbar-inverse navbar-fixed-top">
          <div className="container">
            <div className="navbar-header">
              <a className="navbar-brand" href="/">Priva'Mov Viz</a>
            </div>

            <ul className="nav navbar-nav left">
              <li><Link to="/" activeClassName="active">Dashboard</Link></li>
            </ul>

            <ul className="nav navbar-nav navbar-right">
              <li><a href="http://privamov.liris.cnrs.fr">
                <span className="glyphicon glyphicon-home"/> Project website
              </a></li>
            </ul>
          </div>
        </div>

        {this.props.children}

        <div className="container">
          <footer>
            <span className="glyphicon glyphicon-copyright-mark"/> 2016
            <a href="http://liris.cnrs.fr"><img src="images/liris.png" alt="LIRIS"/></a>
          </footer>
        </div>
      </div>
    );
  }
});

module.exports = App;