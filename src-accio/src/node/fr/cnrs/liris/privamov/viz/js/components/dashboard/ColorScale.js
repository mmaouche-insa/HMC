const React = require('react');
const d3 = require('d3');

let ColorScale = React.createClass({
  getDefaultProps: function () {
    return {
      format: ',s',
      width: 25
    };
  },
  render: function () {
    if (this.props.scale.length === 0) {
      return null;
    }
    const numberFormat = d3.format(this.props.format);
    return (
      <div className={this.props.className}>
        <div style={{marginLeft: '2px'}}>
          {this.props.scale.map((elem, i) => {
            const style = {
              display: 'inline-block',
              width: this.props.width + 'px',
              backgroundColor: elem.color
            };
            return <span key={i} style={style}>&nbsp;</span>;
          })}
        </div>
        <div style={{fontSize: '0.7em'}}>
          {this.props.scale.map((elem, i) => {
            const label = numberFormat(d3.round(elem.from));
            const style = {
              display: 'inline-block',
              width: this.props.width + 'px',
              position: 'relative',
              left: '-' + label.length + 'px'
            };
            return <span key={i} style={style}>{label}</span>;
          })}
        </div>
      </div>
    );
  }
});

ColorScale.propTypes = {
  scale: React.PropTypes.arrayOf(React.PropTypes.object).isRequired,
  format: React.PropTypes.string.isRequired,
  width: React.PropTypes.number.isRequired,
  className: React.PropTypes.string
};

module.exports = ColorScale;