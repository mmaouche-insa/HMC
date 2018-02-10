const React = require('react');
const _ = require('lodash');
const moment = require('moment');
const Highcharts = require('highcharts');

const sample = require('../../lib/sample').sampleN;

let TemporalGraph = React.createClass({
  updateChart: function (data) {
    var distances;
    if (data.features && data.features.length) {
      distances = _.map(sample(data.features, 250), function (feature) {
        return {
          x: Date.parse(feature.properties.time),
          y: Math.round(feature.properties.distance)
        };
      });
      console.log(distances);
    } else {
      distances = [];
    }
    this.chart.series[0].setData(distances);
  },

  componentWillReceiveProps: function (nextProps) {
    this.updateChart(nextProps.data);
  },

  componentDidMount: function () {
    this.chart = Highcharts.chart(this.props.id, {
      title: {
        text: '<b>Distance from starting point.</b> Move your cursor over the chart to reveal the location on the map.',
        align: 'left',
        style: 'font-size: 12px; font-weight: bold'
      },
      /*plotOptions: {
       series: {
       point: {
       events: {
       mouseOver: function () {
       if (this.x.getTime() in markersByTime) {
       map.setView(markersByTime[this.x.getTime()]);
       }
       }
       }
       }
       }
       },*/
      xAxis: {
        type: 'datetime'
      },
      yAxis: {
        floor: 0,
        title: {text: null},
        labels: {enabled: false},
        plotLines: [{value: 0, width: 1, color: '#808080'}]
      },
      tooltip: {
        valueSuffix: 'm',
        crosshairs: [true],
        xDateFormat: '%b %e %G %H:%M:%S',
        shared: true,
        formatter: function () {
          var str = '<b>' + moment(this.x).format('MMM Do, HH:mm') + ':</b> ';
          if (this.y > 10000) {
            return str + this.y / 1000 + ' km';
          }
          return str + this.y + ' m';
        }
      },
      legend: {enabled: false},
      series: [{name: 'Distance', data: []}],
      credits: {enabled: false}
    });
    this.updateChart(this.props.data);
  },

  getDefaultProps: function () {
    return {
      id: _.uniqueId('series')
    };
  },

  render: function () {
    return (
      <div id={this.props.id} className="location-series"></div>
    );
  }
});

module.exports = TemporalGraph;