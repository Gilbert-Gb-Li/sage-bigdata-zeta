'use strict';

/*---------*/

function LineChars(target, args) {
    this.init(target, args);
}
LineChars.prototype.init = function (target, args) {
    this.target = target;
    args = args || {};
    this.canvas = document.createElement('canvas');
    this.canvas.width = 960;
    this.canvas.height = 500;

    this.context = this.canvas.getContext('2d');


    this.margin = args.margin || {top: 20, right: 20, bottom: 30, left: 50};
    this.width = this.canvas.width - this.margin.left - this.margin.right;
    this.height = this.canvas.height - this.margin.top - this.margin.bottom;

    this.x = d3.scaleTime()
        .range([0, this.width]);

    this.y = d3.scaleLinear()
        .range([this.height, 0]);
    let x = this.x;
    let y = this.y;
    this.line = d3.line()
        .x(function (d) {
            return x(d.time);
        })
        .y(function (d) {
            return y(d.cpu);
        })
        .curve(d3.curveStep).context(this.context);


};
LineChars.prototype.xAxis = function () {
    let height = this.height;
    let x = this.x;
    let context = this.context;
    let tickCount = 10,
        tickSize = 6,
        ticks = x.ticks(tickCount),
        tickFormat = x.tickFormat();

    context.beginPath();
    ticks.forEach(function (d) {
        context.moveTo(x(d), height);
        context.lineTo(x(d), height + tickSize);
    });
    context.strokeStyle = 'black';
    context.stroke();
    context.textAlign = 'center';
    context.textBaseline = 'top';
    ticks.forEach(function (d) {
        context.fillText(tickFormat(d), x(d), height + tickSize);
    });
};

LineChars.prototype.yAxis = function () {
    let height = this.height;
    let y = this.y;
    let context = this.context;
    let tickCount = 10,
        tickSize = 6,
        tickPadding = 3,
        ticks = y.ticks(tickCount),
        tickFormat = y.tickFormat(tickCount);

    context.beginPath();
    ticks.forEach(function (d) {
        context.moveTo(0, y(d));
        context.lineTo(-6, y(d));
    });
    context.strokeStyle = 'black';
    context.stroke();

    context.beginPath();
    context.moveTo(-tickSize, 0);
    context.lineTo(0.5, 0);
    context.lineTo(0.5, height);
    context.lineTo(-tickSize, height);
    context.strokeStyle = 'black';
    context.stroke();

    context.textAlign = 'right';
    context.textBaseline = 'middle';
    ticks.forEach(function (d) {
        context.fillText(tickFormat(d), -tickSize - tickPadding, y(d));
    });

    context.save();
    context.rotate(-Math.PI / 2);
    context.textAlign = 'right';
    context.textBaseline = 'top';
    context.font = 'bold 10px sans-serif';
    context.fillText('Price (US$)', -10, 10);
    context.restore();
};
LineChars.prototype.render = function (data) {
    this.context.translate(this.margin.left, this.margin.top);
    let parseTime = d3.timeParse('%d-%b-%y');
    let context = this.context;
    this.x.domain(d3.extent(data, function (d) {
        return parseTime(d.time);
    }));
    this.y.domain(d3.extent(data, function (d) {
        return +d.cpu;
    }));

    this.xAxis();
    this.yAxis();

    context.beginPath();
    this.line(data);
    context.lineWidth = 1.5;
    context.strokeStyle = 'steelblue';
    context.stroke();
    $(this.canvas).appendTo(this.target);
};

function BaseChart(target, oArgs) {
    this.init(target, oArgs);
}

BaseChart.prototype.init = function (target, oArgs) {
    this.target = target;
    this.context = target;

    oArgs = oArgs || {};
    this.width = oArgs.width;
    this.height = oArgs.height;
    this.margin = oArgs.margin || 0;
};

BaseChart.prototype.empty = function () {
    let now = Date.now(),
        yesterday = now - (60 * 60 * 1000);
    let values = [{
        time: yesterday,
        cpu: 0
    }, {
        time: now,
        cpu: 1
    }];

    let xScale = this.getXScale(values);
    let yScale = this.getYScale(values);

    let xAxis = this.getXAxis(xScale);
    let yAxis = this.getYAxis(yScale);

    let svg = d3.select(this.target).append('svg')
        .attr('width', this.width)
        .attr('height', this.height);

    svg.append('g')
        .attr('class', 'x axis')
        .attr('transform', 'translate(0, ' + (this.height - this.margin) + ')')
        .call(xAxis);

    d3.selectAll('.x.axis .tick text')
        .call(this.xAxisLabel, xScale);

    svg.append('g')
        .attr('class', 'y axis')
        .attr('transform', 'translate(' + this.margin + ', 0)')
        .call(yAxis);

    return svg;
};


function TimeSeries(target, oArgs) {
    this.init(target, oArgs);
}
TimeSeries.prototype = new BaseChart();

TimeSeries.prototype.render = function (values) {
    if (values.length === 0) {
        return this.empty();
    }

    let xScale = this.getXScale(values);
    let yScale = this.getYScale(values);

    let xAxis = this.getXAxis(xScale);
    let yAxis = this.getYAxis(yScale);

    let line = this.lineGenerator(xScale, yScale);

    let svg = d3.select(this.target).append('svg')
        .attr('width', this.width)
        .attr('height', this.height);

    svg.append('g')
        .attr('class', 'x axis')
        .attr('transform', 'translate(0, ' + (this.height - this.margin) + ')')
        .call(xAxis);

    svg.selectAll('.x.axis .tick text')
        .call(this.xAxisLabel, xScale);

    svg.append('g')
        .attr('class', 'y axis')
        .attr('transform', 'translate(' + this.margin + ', 0)')
        .call(yAxis);

    svg.append('path')
        .attr('d', line(values));

    svg.selectAll('.dot')
        .data(values)
        .enter().append('circle')
        .attr('class', 'dot')
        .attr('r', 3.5)
        .attr('cx', function (d) {
            return xScale(d.time);
        })
        .attr('cy', function (d) {
            return yScale(d.cpu);
        })
        .on('mouseover', function (el) {
            d3.select(this)
                .attr('r', 5);
        })
        .on('mouseout', function (el) {
            d3.select(this)
                .attr('r', 3.5);
        });

    return svg;
};

TimeSeries.prototype.getXScale = function (values) {
    return d3.scaleTime()
        .domain([values[0].time, values[values.length - 1].time])
        .range([0 + this.margin, this.width]);
};

TimeSeries.prototype.getYScale = function (values) {
    return d3.scaleLinear()
        .range([this.height - this.margin, 0])
        .domain([0, d3.max(values, function (d) {
            return d.cpu;
        })]).nice();
};

TimeSeries.prototype.getXAxis = function (scale) {
    return d3.axisBottom(scale)
        .ticks(d3.timeMinutes, 15);
};

TimeSeries.prototype.getYAxis = function (scale) {
    return d3.axisLeft(scale)
        .ticks(6)
        .tickFormat(d3.format('.0%'));
};

TimeSeries.prototype.lineGenerator = function (xScale, yScale) {
    return d3.line()
        .x(function (d) {
            return xScale(d.time);
        })
        .y(function (d) {
            return yScale(d.cpu);
        });
};

TimeSeries.prototype.xAxisLabel = function (scale) {
    /*
     * Override this method to customize X Axis labels.
     */
    let timeFormat = d3.time.format('%I:%M %p');
    let dateFormat = d3.time.format('%d %b');

    scale.each(function (d) {
        let text = d3.select(this),
            content = text.text();
        text.text(null)
            .append('tspan')
            .attr('x', 0)
            .text(timeFormat(d));
        text.append('tspan')
            .attr('x', 0)
            .attr('dy', 13).text(dateFormat(d));
    });
};

function SparkLine(target, oArgs) {
    this.init(target, oArgs);
}
SparkLine.prototype = new TimeSeries();

SparkLine.prototype.getXScale = function (values) {
    return d3.scaleTime()
        .domain([values[0].time, values[values.length - 1].time])
        .range([0, this.width]);
};

SparkLine.prototype.getYScale = function (values) {
    return d3.scaleLinear()
        .range([this.height, 0])
        .domain(d3.extent(values, function (d) {
            return d.cpu;
        }));
};

SparkLine.prototype.render = function (values) {
    let xScale = this.getXScale(values);
    let yScale = this.getYScale(values);

    let xAxis = this.getXAxis(xScale);
    let yAxis = this.getYAxis(yScale);

    let line = this.lineGenerator(xScale, yScale);

    let svg = d3.select(this.target).append('svg')
        .attr('width', this.width)
        .attr('height', this.height)
        .append('path')
        .datum(values)
        .attr('class', 'sparkline')
        .attr('d', line(values));

    return svg;
};

app.factory('TimeSeriesChart', function () {
    return {
        getChart: function (target, oArgs) {
            return new LineChars(target, oArgs);
        }
    };
}).factory('SparkLineChart', function () {
    return {
        getChart: function (target, oArgs) {
            return new SparkLine(target, oArgs);
        }
    };
});
