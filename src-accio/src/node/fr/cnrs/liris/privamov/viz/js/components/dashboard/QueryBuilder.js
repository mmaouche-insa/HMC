const React = require('react');
const _ = require('lodash');
const moment = require('moment');
const Grid = require('react-bootstrap/lib/Grid');
const Row = require('react-bootstrap/lib/Row');
const Col = require('react-bootstrap/lib/Col');
const FormGroup = require('react-bootstrap/lib/FormGroup');
const FormControl = require('react-bootstrap/lib/FormControl');
const ControlLabel = require('react-bootstrap/lib/ControlLabel');
const Button = require('react-bootstrap/lib/Button');
const Glyphicon = require('react-bootstrap/lib/Glyphicon');
const Form = require('react-bootstrap/lib/Form');
const DateTime = require('react-datetime');

const resolutions = require('../../constants/resolutions');

let QueryBuilder = React.createClass({
  getDefaultProps: function () {
    return {
      onSubmit: _.noop,
      datasets: []
    };
  },

  getInitialState: function () {
    const now = moment();
    return {
      dataset: undefined,
      start_after: now.clone().subtract(7, 'days'),
      end_before: now,
      resolution: 13
    };
  },

  _handleDatasetChange: function (e) {
    this.setState({dataset: e.target.value});
  },

  _handleStartAfterChange: function (value) {
    this.setState({start_after: value});
  },

  _handleEndBeforeChange: function (value) {
    this.setState({end_before: value});
  },

  _handleResolutionChange: function (e) {
    this.setState({resolution: parseInt(e.target.value)});
  },

  _handleSubmit: function () {
    //TODO: validate everything is valid (especially a dataset is selected).
    this.props.onSubmit({
      dataset: this.state.dataset.name,
      start_after: this.state.start_after,
      end_before: this.state.end_before,
      resolution: this.state.resolution
    });
  },

  _isValidDate: function (date) {
    if (!this.state.dataset) {
      return true;
    } else {
      return (date.isSame(this.state.dataset.from) || date.isAfter(this.state.dataset.from))
        && (date.isSame(this.state.dataset.to) || date.isBefore(this.state.dataset.to));
    }
  },

  componentWillReceiveProps: function (nextProps) {
    if (!this.state.dataset && nextProps.datasets.length > 0) {
      // No dataset was defined and we have a list of datasets, we select by default the first one.
      const dataset = nextProps.datasets[0];
      const startAfter = moment.max(dataset.from, dataset.to.clone().subtract(7, 'days'));
      const changes = {
        dataset: dataset,
        start_after: startAfter,
        end_before: dataset.to
      };
      this.setState(changes);
    } else if (this.state.dataset && !_.find(nextProps.datasets, elem => elem.name == this.state.dataset.name)) {
      // Actually selected dataset does not exist in the new list of datasets, we unselect it.
      this.setState({dataset: undefined});
    }
  },

  shouldComponentUpdate: function (nextProps, nextState) {
    return !_.isEqual(this.props.datasets, nextProps.datasets) || !_.isEqual(this.state, nextState);
  },

  render: function () {
    return (
      <Grid>
        <Row>
          <Form>
            <Row style={{display: 'flex'}}>
              <Col sm={2}>
                <FormGroup>
                  <ControlLabel>Dataset:</ControlLabel>
                  <FormControl componentClass="select"
                               value={this.state.dataset ? this.state.dataset.name : undefined}
                               onChange={this._handleDatasetChange}>
                    {this.props.datasets.map((elem, i) =>
                      <option key={i} value={elem.name}>{elem.name}</option>)}
                  </FormControl>
                </FormGroup>
              </Col>
              <Col sm={2}>
                <FormGroup>
                  <ControlLabel>Date from:</ControlLabel>
                  <DateTime value={this.state.start_after}
                            onChange={this._handleStartAfterChange}
                            isValidDate={this._isValidDate}/>
                </FormGroup>
              </Col>
              <Col sm={2}>
                <FormGroup>
                  <ControlLabel>Date to:</ControlLabel>
                  <DateTime value={this.state.end_before}
                            onChange={this._handleEndBeforeChange}
                            isValidDate={this._isValidDate}/>
                </FormGroup>
              </Col>
              <Col sm={2}>
                <FormGroup>
                  <ControlLabel>Resolution:</ControlLabel>
                  <FormControl componentClass="select"
                               value={this.state.resolution}
                               onChange={this._handleResolutionChange}>
                    {resolutions.map((elem, i)=>
                      <option key={i} value={elem.key}>{elem.label}</option>)}
                  </FormControl>
                </FormGroup>
              </Col>
              <Col sm={2}>
                <Button bsStyle="primary"
                        bsSize="large"
                        block
                        style={{marginTop: '5px'}}
                        onClick={this._handleSubmit}>
                  <Glyphicon glyph="ok"/>&nbsp;Query
                </Button>
              </Col>
            </Row>
          </Form>
        </Row>
      </Grid>
    );
  }
});

QueryBuilder.propTypes = {
  onSubmit: React.PropTypes.func.isRequired,
  datasets: React.PropTypes.arrayOf(React.PropTypes.object).isRequired
};


module.exports = QueryBuilder;