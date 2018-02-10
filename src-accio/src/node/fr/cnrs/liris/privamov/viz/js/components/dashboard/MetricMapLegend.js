const React = require('react');
const ColorScale = require('./ColorScale');

let MetricMapLegend = React.createClass({
  render: function () {
    return (
      <div style={{backgroundColor: 'white', padding: '5px', width: '350px'}}>
        <div style={{fontSize: '1.2em', fontWeight: 'bold', textAlign: 'center', borderBottom: '1px solid #d1d1d1', paddingBottom: '5px'}}>
          Heatmap of events
        </div>
        <div style={{marginTop: '5px'}}>
          <ColorScale scale={this.props.scale} />
        </div>
        <div>Period shown:</div>
      </div>
    );
  }
});

MetricMapLegend.propTypes = {
  scale: React.PropTypes.arrayOf(React.PropTypes.object).isRequired,
  startAfter: React.PropTypes.object,
  endBefore: React.PropTypes.object
};

module.exports = MetricMapLegend;