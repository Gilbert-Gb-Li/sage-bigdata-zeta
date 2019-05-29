'use strict';

app.controller('MetricsController', function ($scope, Metrics, DataSource) {
    $scope.options = {
        chart: {
            type: 'lineChart',
            height: 450,
            margin: {
                top: 20,
                right: 20,
                bottom: 40,
                left: 55
            },
            x: function (d) {
                return d3.time.format('%Y-%m-%d %H:%M:%S.%L').parse(d.x);
            },
            y: function (d) {
                return d.y;
            },
            useInteractiveGuideline: true,
            dispatch: {
                stateChange: function (e) {
                    console.log('stateChange');
                },
                changeState: function (e) {
                    console.log('changeState');
                },
                tooltipShow: function (e) {
                    console.log('tooltipShow');
                },
                tooltipHide: function (e) {
                    console.log('tooltipHide');
                }
            },
            xAxis: {
                axisLabel: '时间(ms)',
                tickFormat: function (d) {
                    return d3.time.format('%H:%M:%S')(new Date(d))
                }
            },
            yAxis: {
                axisLabel: '条数/秒',
                tickFormat: function (d) {
                    return d;
                },
                axisLabelDistance: -10
            },
            callback: function (chart) {
                console.log('!!! lineChart callback !!!');
            }
        },
        title: {
            enable: true,
            text: '采集进度'
        },
        subtitle: {
            enable: true,
            text: '显示每一个采集器,不同数据源的数据收集情况',
            css: {
                'text-align': 'center',
                'margin': '10px 13px 0px 7px'
            }
        },
        caption: {
            html: '<b>1.</b> X轴标示采集的数据条数, <span style=\'text-decoration: underline;\'>EPS</span>,<i>Cum in purto erat, mea ne nominavi persecuti reformidans.</i> Docendi blandit abhorreant ea has, minim tantas alterum pro eu. <span style=\'color: darkred;\'>Exerci graeci ad vix, elit tacimates ea duo</span>. Id mel eruditi fuisset. Stet vidit patrioque in pro, eum ex veri verterem abhorreant, id unum oportere intellegam nec<sup>[1, <a href=\'https://github.com/krispo/angular-nvd3\' target=\'_blank\'>2</a>, 3]</sup>.',
            enable: true,
            css: {
                'text-align': 'justify',
                'margin': '10px 13px 0px 7px'
            }
        }
    };
    setInterval(function () {
        $scope.data = [];
        DataSource.get({}, function (configs) {
            let now = new Date();
            let to = d3.time.format('%Y-%m-%d %H:%M:%S.%L')(now);
            let from = d3.time.format('%Y-%m-%d %H:%M:%S.%L')(new Date(now.getTime() - 12 * 3600 * 1000));
            console.log(from);
            console.log(to);
            configs.result.forEach(function (config) {

                Metrics.query({collector: config.collectorId, id: config.id, from: from, to: to}, function (metrics) {
                    let mrsts = [];
                    if (!metrics.status) {
                        metrics.forEach(function (metric) {
                            if (metric.metricType === 'METERS') {
                                let d = {
                                    x: metric.datetime,
                                    y: Number(metric.metric['mean rate'].split(' ')[0])
                                };

                                mrsts.push(d);
                            }

                        });
                        if (mrsts.length > 0) {
                            $scope.data.push({
                                key: config.name,
                                values: mrsts
                            });

                        }
                    }
                });
            });
            //    $scope.$apply();

        });
    }, 10000);

});
