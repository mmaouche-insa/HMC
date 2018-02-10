var React = require('react');
var _ = require('lodash');
var LinkContainer = require('react-router-bootstrap').LinkContainer;
var Navbar = require('react-bootstrap/lib/Navbar');
var Nav = require('react-bootstrap/lib/Nav');
var NavItem = require('react-bootstrap/lib/NavItem');
var Row = require('react-bootstrap/lib/Row');

var AppController = React.createClass({
  getInitialState: function () {
    return {showModal: false, context: {}};
  },

  render: function () {
    return (
      <div>
        <Navbar inverse fixedTop>
          <Navbar.Header>
            <Navbar.Brand>
              <a href="/">Priva'Mov Viz</a>
            </Navbar.Brand>
          </Navbar.Header>
          <Nav>
            <LinkContainer to="/">
              <NavItem>Dashboard</NavItem>
            </LinkContainer>
          </Nav>
        </Navbar>

        {this.props.children}

        <Row>
          <footer>
            <span className="glyphicon glyphicon-copyright-mark"/> 2016
            <a href="http://liris.cnrs.fr"><img src="images/liris.png" alt="LIRIS"/></a>
          </footer>
        </Row>
      </div>
    );
  }
});

module.exports = AppController;