const React = require('react');

let LocationTransform = React.createClass({
  handleRawClick: function (e) {
    e.preventDefault();
    this.setState({active: 'raw'});
    this.props.onChange();
  },

  handleSmoothClick: function (e) {
    e.preventDefault();
    this.setState({active: 'smooth'});
  },

  handleGeoindClick: function (e) {
    e.preventDefault();
    this.setState({active: 'geoind'});
  },

  handleSmoothChange: function (e) {
    e.preventDefault();
    this.props.onChange('smooth', e.target.value + '.meters');
  },

  handleGeoindChange: function (e) {
    e.preventDefault();
    this.props.onChange('geoind', e.target.value);
  },

  getInitialState: function () {
    return {active: 'raw'};
  },

  getDefaultProps: function () {
    return {
      onChange: _.noop()
    };
  },

  render: function () {
    return (
      <div className="panel-group">
        <div className="panel panel-default">
          <div className="panel-heading">
            <h4 className="panel-title">
              <a href="#" onClick={this.handleRawClick}>
                <span className="glyphicon glyphicon-pushpin"/> Raw data
              </a>
            </h4>
          </div>
        </div>

        <div className="panel panel-default">
          <div className="panel-heading">
            <h4 className="panel-title">
              <a href="#" onClick={this.handleGeoindClick}>
                <span className="glyphicon glyphicon-map-marker"/> Geo-I
              </a>
            </h4>
          </div>
          <div
            className={this.state.active === 'geoind' ? 'panel-collapse' : 'panel-collapse collapse'}>
            <div className="panel-body">
              <input type="text"
                     className="form-control"
                     placeholder="Epsilon"
                     onBlur={this.handleGeoindChange}/>
            </div>
          </div>
        </div>

        <div className="panel panel-default">
          <div className="panel-heading">
            <h4 className="panel-title">
              <a href="#" onClick={this.handleSmoothClick}>
                <span className="glyphicon glyphicon-road"/> Speed smoothing
              </a>
            </h4>
          </div>
          <div
            className={this.state.active === 'smooth' ? 'panel-collapse' : 'panel-collapse collapse'}>
            <div className="panel-body">
              <input type="text"
                     className="form-control"
                     placeholder="Distance"
                     onBlur={this.handleSmoothChange}/>
            </div>
          </div>
        </div>
      </div>
    );
  }
});

LocationTransform.propTypes = {
  onChange: React.PropTypes.func
};

module.exports = LocationTransform;