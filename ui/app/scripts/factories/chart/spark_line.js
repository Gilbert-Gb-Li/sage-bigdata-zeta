'use strict';

app.factory('SparkLineCharts', function () {
    return function () {
        return {
            key: 'key',
            val: 'count',
            width: 400,
            height: 280,
            num: 0,//标识为第几个chart
            colors: ['#92A2A8'],
            margin: {top: 5, right: 5, bottom: 5, left: 5},
            showX: false,
            showY: false,
            title: '',
            legend: '',
            xRotate: null,
            draw: function (element, data) {
                let key = this.key;
                let val = this.val;
                let width = this.width;
                let height = this.height;
                let num = this.num;
                let colors = this.colors;
                let margin = this.margin;
                let legend = this.legend;

                let color = d3.scale.ordinal().range(colors);
                color.domain(d3.keys(data[0]).filter(function (key) {
                    return key !== 'key';
                }));

                let parentId = '#sparkline_' + num;
                if (d3.select(parentId)) {
                    d3.select(parentId).remove();
                }

                d3.select(element).append('div').attr('id', 'sparkline_' + num).attr('class', 'hs_sparklineCharts');

                /**判断是否显示标题**/
                if (this.title != null && $.trim(this.title) != '') {
                    let p_title = d3.select(parentId).append('div').attr('class', 'hs_charts_title').text(this.title);
                    height = height - p_title.style('height').substring(0, p_title.style('height').indexOf('px')) - 5;
                }

                /**如果显示Y轴，则计算y轴宽度**/
                if (this.showY) {
                    let n = 0;
                    let len = 0;
                    for (let i in data) {
                        len = data[i][val].toString().length;
                        n = n < len ? len : n;
                    }
                    margin.left += n * 8 + 10;
                }
                /**如果显示X轴，则计算x轴高度**/
                let xHeight = 0;
                if (this.showX) {
                    let n = 0;
                    let len = 0;
                    for (let i in data) {
                        len = data[i][key].toString().length;
                        n = n < len ? len : n;
                    }
                    xHeight = n * 6;
                    xHeight = xHeight < 25 ? 25 : xHeight;
                }

                /**如果显示图例**/
                if (legend !== null && legend !== '' && legend !== 'none') {
                    let desc = d3.select(parentId).append('div')
                        .attr('class', 'panel_sparkline_desc')
                        .attr('id', 'sparkline_desc_' + num);

                    let descs = d3.select('#sparkline_desc_' + num).append('div:ul').attr('class', 'panel_sparkline_desc_' + num + ' hs_charts_desc_ul');
                    descs.selectAll('ul.panel_sparkline_desc_' + num).data(data).enter().append('ul.panel_sparkline_desc_' + num + ':li')
                        .attr('class', 'icon-circle hs_left hs_charts_desc_ul_li')
                        .style('color', function (d, i) {
                            return color(i);
                        })
                        .text(function (d, i) {
                            return d[key] + ' (' + d[val] + ') ';
                        });

                    height = height - descs.style('height').substring(0, descs.style('height').indexOf('px')) - 5;
                }
                if (legend == 'below')
                    desc.remove();
                /**计算宽度**/
                let canvasWidth = width - margin.left - margin.right;
                let canvasHeight = height - margin.top - margin.bottom;

                /**如果显示X轴，重新计算画布高度**/
                if (this.showX) {
                    canvasHeight -= (xHeight / 2);
                }
                let x = d3.scale.ordinal().domain(d3.range(data.length)).rangeRoundBands([0, canvasWidth]);
                let y = d3.scale.linear().domain([0, d3.max(data, function (d) {
                    return d[val];
                })]).range([canvasHeight, 0]);

                let svg = d3.select(parentId)
                    .append('svg:svg')
                    .attr('width', '100%')
                    .attr('height', height)
                    .attr('id', 'sparkline_charts_' + num)
                ;

                let cities = color.domain().map(function (name) {
                    return {
                        name: name,
                        values: data.map(function (d) {
                            return {key: d.key, temperature: +d[name]};
                        })
                    };
                });

                let line = d3.svg.line()
                    .interpolate('linear')
                    .x(function (d, i) {
                        return x.rangeBand() * (i + 0.5);
                    })
                    .y(function (d) {
                        return y(d.temperature);
                    });
                let city = svg.selectAll('.city')
                    .data(cities)
                    .enter().append('g')
                    .attr('class', 'city');

                city.append('path')
                    .attr('class', 'line')
                    .attr('d', function (d) {
                        return line(d.values);
                    })
                    .style('stroke', function (d) {
                        return color(d.name);
                    })
                    .style('fill', 'none')
                    .attr('transform', 'translate(' + margin.left + ',' + margin.top + ')');

//					city.append('text')
//					  .datum(function(d) { return {name: d.name, value: d.values[d.values.length - 1]}; })
//					  .attr('transform', function(d,i) { return 'translate(' + (canvasWidth-x.rangeBand()/2+2) + ',' + y(d.value.temperature) + ')'; })
//					  .attr('x', 3)
//					  .attr('dy', '.35em')
//					  .attr('fill',function(d,i){return color(cities[i].name);})
//					  .text(function(d) { return d.name; });

                for (let i = 0; i < cities.length; i++) {
                    let pointer = svg.selectAll('.point')
                        .data(cities[i].values)
                        .enter().append('svg:circle');
                    pointer.attr('class', 'chart_point')
                        .attr('stroke', color(cities[i].name))
                        .attr('fill', color(cities[i].name))
                        .attr('cx', function (d, i) {
                            return x.rangeBand() * (i + 0.5);
                        })
                        .attr('cy', function (d, i) {
                            return y(d.temperature);
                        })
                        .attr('r', function (d, i) {
                            return 3;
                        })
                        .attr('transform', 'translate(' + margin.left + ',' + margin.top + ')')
                    ;
                    pointer.on('mouseover', function (d, i) {
                        d3.select('#sparkline_popup_' + num)
                            .html(function () {
                                return d.key + '<br/>' + d.temperature;
                            })
                            .style('left', (d3.event.pageX - $(parentId).offset().left) + 'px')
                            .style('top', (d3.event.pageY - $(parentId).offset().top - $(parentId).height() - margin.top - margin.bottom) + 'px')
                            .attr('class', 'hs_sparkline_popup')
                        ;
                        return d3.select(this).attr('r', 4);
                    }).on('mouseout', function (d, i) {
                        d3.select('#sparkline_popup_' + num)
                            .on('mouseover', function (d, i) {
                                $(this).attr('class', 'hs_sparkline_popup')
                            })
                            .on('mouseout', function (d, i) {
                                $(this).attr('class', 'hs_hide')
                            })
                            .attr('class', 'hs_hide')
                        ;
                        return d3.select(this).attr('r', 3);
                    });
                }

                /**x,y轴**/
                if (this.showX) {
                    let xAxis = d3.svg.axis()
                        .scale(x)
                        .orient('bottom')
                        .tickFormat(function (d) {
                            return data[d].key
                        })
                    ;
                    let ag = svg.append('g')
                        .attr('class', 'hs_charts_x_axis')
                        .attr('transform', 'translate(' + margin.left + ',' + (canvasHeight + margin.top) + ')')
                        .call(xAxis);
                    if (!!this.xRotate) {
                        ag.selectAll('text')
                            .attr('transform', 'translate(0,' + (xHeight / (90 / this.xRotate) / 2) + '),rotate(-' + this.xRotate + ')')
                        ;
                    }
                }
                if (this.showY) {

                    let yAxis = d3.svg.axis()
                        .scale(y)
                        .orient('left')
                        .ticks(5);

                    svg.append('g')
                        .attr('class', 'hs_charts_y_axis')
                        .attr('transform', 'translate(' + margin.left + ',' + margin.top + ')')
                        .call(yAxis)
                        .append('text')
                        .attr('transform', 'rotate(-90)')
                        .attr('y', 6)
                        .attr('dy', '.71em')
                        .style('text-anchor', 'end')
//						  .text('url数量')
                    ;
                }

                if (legend === 'below') {
                    let desc = d3.select(parentId).append('div')
                        .attr('class', 'panel_sparkline_desc')
                        .attr('id', 'sparkline_desc_' + num);

                    let descs = d3.select('#sparkline_desc_' + num).append('div:ul').attr('class', 'panel_sparkline_desc_' + num + ' hs_charts_desc_ul');
                    descs.selectAll('ul.panel_sparkline_desc_' + num).data(data).enter().append('ul.panel_sparkline_desc_' + num + ':li')
                        .attr('class', 'icon-circle hs_left hs_charts_desc_ul_li')
                        .style('color', function (d, i) {
                            return color(i);
                        })
                        .text(function (d, i) {
                            return d[key] + ' (' + d[val] + ') ';
                        });
                }

                d3.select(parentId).append('div').attr('id', 'sparkline_popup_' + num).attr('class', 'hs_hide');
            }
        };
    }
});
