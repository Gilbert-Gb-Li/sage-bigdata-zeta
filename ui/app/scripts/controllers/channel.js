'use strict';

app.controller('ChannelController', function ($scope, $rootScope, $timeout, $location, Channel, Modeling, Analyzer, Util, DataSource, Metrics, Collector, Parser, Writer) {
    $rootScope.$path = $location.path.bind($location);
    $scope.encodings = ['UTF-8', 'GBK', 'GB18030', 'GB2312', 'BIG5', 'UNICODE', 'ASCII', 'ISO-8859-1'];
    const $confirmModal = $('#confirmModal');
    $scope.countMessage = '采集的总量：未知';
    const d3_format = d3.time.format('%Y-%m-%d %H:%M:%S');
    let days = 10 * 60 * 1000;

    let defaultChannel = {
        type: {
            name: "parser"
        },
        properties: {
            timeout_ms: 100,
            polling_ms: 1000
        },
        writers: []
    };
    $scope.channel = defaultChannel;
    //采集器
    $scope.collectors = Collector.query();
    //数据源
    $scope.datasources = DataSource.byType({type: "default"});

    let defaultChannels = DataSource.byType({type: "channel"});
    //数据通道
    $scope.channels = defaultChannels;

    //解析规则
    $scope.resolvers = Parser.query();
    $scope.resolverList = $scope.resolvers;
    //分析规则
    $scope.analyzers = Analyzer.query();

    //数据源实体
    $scope.datasource = {};
    //获得数据源详情
    $scope.changeDataSourceType = function (id) {
        DataSource.get({
            id: id
        }, function (ds) {
            $scope.datasource = ds;
        });
    };

    $scope.connectTest = function (valid) {
        var workerId = $scope.channel.collector;
        var url = $scope.channel.type.cluster;
        if (valid) {
            angular.element("#connect_test").html('<i class="fa fa-spinner fa-spin"></i>');
            Modeling.connectTest({opt: 'check', workerId: workerId, url: url}, function (rt) {
                $scope.message = rt;
                angular.element("#connect_test").html('测试连接');
            }, function (error) {
                let content = "Connect to server has timed out, please try again later.";
                $scope.message = {
                    message: content
                };
                angular.element("#connect_test").html('测试连接');
            });
        }
    };

    //数据存储
    $scope.writers = Writer.query();
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
                return d.x;
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
                axisLabel: '时间',
                tickFormat: function (d) {
                    return d3.time.format('%d日%H:%M:%S')(new Date(d))
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
            text: '采集时间范围',
            css: {
                'text-align': 'center',
                'margin': '10px 13px 0px 7px'
            }
        },
        caption: {
            html: ' ',
            enable: true,
            css: {
                'text-align': 'justify',
                'margin': '10px 13px 0px 7px'
            }
        }
    };

    $scope.usableCheck = function () {
        let data = angular.copy($scope.channel);
        data.writers = setForSave(data.writers);
        Channel.check(data, function (rt) {
            $scope.rt = rt;
        });
    };


    $scope.metric = {
        interval: 5,
        format: 'yyyy-MM-dd HH:mm:ss',
        altInputFormats: ['yyyy/M!/d!'],
        popup1: {
            opened: false
        },
        open1: function () {
            this.popup1.opened = true;
        },
        popup2: {
            opened: false
        },
        open2: function () {
            this.popup2.opened = true;
        }
    };

    function getWriterNameById(id) {
        var writes_name = {}
        $scope.writers.forEach(function (w) {
            writes_name[w.id] = w.name
        });
        if (writes_name[id]) {
            return writes_name[id]
        } else {
            return id
        }
    }

    function getChannelNameById(id) {
        var channels_name = {}
        $scope.channels.forEach(function (w) {
            channels_name[w.id] = w.name
        });
        if (channels_name[id]) {
            return channels_name[id]
        } else {
            return id
        }
    }

    function metricsOne(id, startTime, endTime) {
        let channelTypeName = '';


        if ($scope.channel.type.name === 'parser')
            channelTypeName = '解析';
        else if ($scope.channel.type.name === 'analyzer')
            channelTypeName = '分析';

        Channel.get({id: id}, function (config) {

            $scope.options.title.text = config.name;

            $scope.options.subtitle.html = '从【' + startTime + '】到 【' + endTime + '】';

            Metrics.query({
                collector: config.collector,
                id: config.id,
                from: startTime,
                to: endTime,
                interval: $scope.metric.interval
            }, function (wrapper) {
                let format=d3.time.format('%Y-%m-%d %H:%M:%S');

                $scope.data = [];
                let metrics = wrapper.info;
                let xName = [{x: format.parse(startTime)},
                    {x: format.parse(endTime)}];
                let read_success = xName.concat([]);
                let operation_in = xName.concat([]);
                let operation_success = xName.concat([]);
                let operation_fail = xName.concat([]);
                let operation_ignore = xName.concat([]);
                let write_success = {};
                let flink_in = {};
                let flink_out = {};
                let counts = [];
                let count = 0;
                var countMsg = {};
                var metric_type = '1-minute rate';
                if (!metrics.status) {
                    /*nvd3 bug 时间必须正序加载*/
                    metrics/*.reverse()*/.forEach(function (metric) {
                        let date = format.parse(metric.lasttime);
                        // console.log(date);
                        /*let date = new Date(now.getTime()-3600000 - i * 1000);

                         console.log(dat1);
                         console.log(2);
                         console.log(date);
                         i++;*/
                        if (metric.metricType === "SUCCESS" && metric.metricPhase === "READ") {
                            read_success.push({
                                x: date,
                                y: metric.metric[metric_type]
                            });
                            // let _count = metric.metric['count'];
                        }
                        if ($scope.channel.type.name == 'parser') {
                            if (metric.metricType == "IN" && metric.metricPhase == "OPERATION") {
                                operation_in.push({
                                    x: date,
                                    y: metric.metric[metric_type]
                                });
                            }
                            if (metric.metricType == "SUCCESS" && metric.metricPhase == "OPERATION") {
                                operation_success.push({
                                    x: date,
                                    y: metric.metric[metric_type]
                                });
                            }
                            if (metric.metricType == "FAIL" && metric.metricPhase == "OPERATION") {
                                operation_fail.push({
                                    x: date,
                                    y: metric.metric[metric_type]
                                });
                            }
                            if (metric.metricType == "IGNORE" && metric.metricPhase == "OPERATION") {
                                operation_ignore.push({
                                    x: date,
                                    y: metric.metric[metric_type]
                                });
                            }
                        }
                        if (metric.metricType == "SUCCESS" && metric.metricPhase == "WRITE" && metric.metricId.indexOf("forward") == -1) {
                            var write_key = getWriterNameById(metric.metricId.split("@")[0]);
                            if (!write_success[write_key]) {
                                write_success[write_key] = xName.concat([]);
                            }
                            write_success[write_key].push({
                                x: date,
                                y: metric.metric[metric_type]
                            });
                        }
                        if (metric.metricType == "SUCCESS" && metric.metricPhase == "FLINKIN") {
                            var flink_in_key = getChannelNameById(metric.metricId.split("@")[0]);
                            if (!flink_in[flink_in_key]) {
                                flink_in[flink_in_key] = xName.concat([]);
                            }
                            flink_in[flink_in_key].push({
                                x: date,
                                y: metric.metric[metric_type]
                            });
                        }
                        if (metric.metricType == "SUCCESS" && metric.metricPhase == "FLINKOUT") {
                            var flink_out_key = getChannelNameById(metric.metricId.split("@")[0]);
                            if (!flink_out[flink_out_key]) {
                                flink_out[flink_out_key] = xName.concat([]);
                            }
                            flink_out[flink_out_key].push({
                                x: date,
                                y: metric.metric[metric_type]
                            });
                        }

                        /* counts.push({
                         x: metric.datetime,
                         y: Math.log(Number(metric.metric['count']))
                         });*/

                    });
                    /*$scope.options.caption.html = '<p><b></b> 已经采集的总量：<span style='text-decoration: underline;'><b>' + count + '</b></span>条';*/

                    for (var key in wrapper.count) {
                        if (key.indexOf("read_success") != -1) {
                            if(!countMsg['read']) countMsg['read'] = {};
                            countMsg['read']['success'] = wrapper.count[key];
                        }
                        else if (key.indexOf("operation_in") != -1) {
                            if(!countMsg['operation']) countMsg['operation'] = {};
                            countMsg['operation']['in'] = wrapper.count[key];
                        }
                        else if (key.indexOf("operation_success") != -1) {
                            if(!countMsg['operation']) countMsg['operation'] = {};
                            countMsg['operation']['success'] = wrapper.count[key];
                        }
                        else if (key.indexOf("operation_fail") != -1) {
                            if(!countMsg['operation']) countMsg['operation'] = {};
                            countMsg['operation']['fail'] = wrapper.count[key];
                        }
                        else if (key.indexOf("operation_ignore") != -1) {
                            if(!countMsg['operation']) countMsg['operation'] = {};
                            countMsg['operation']['ignore'] = wrapper.count[key];
                        }
                        else if (key.indexOf("write_success") != -1 && key.indexOf("forward") == -1) {
                            var write_key = getWriterNameById(key.split("@")[0]);
                            if(!countMsg['writes']) countMsg['writes'] = {};
                            if(!countMsg['writes'][write_key]) countMsg['writes'][write_key] = {};
                            countMsg.writes[write_key]["success"] = wrapper.count[key];
                        }
                        else if (key.indexOf("write_fail") != -1 && key.indexOf("forward") == -1) {
                            var write_key = getWriterNameById(key.split("@")[0]);
                            if(!countMsg['writes']) countMsg['writes'] = {};
                            if(!countMsg['writes'][write_key]) countMsg['writes'][write_key] = {};
                            countMsg.writes[write_key]["fail"] = wrapper.count[key];
                        }
                        else if (key.indexOf("flinkin_success") != -1) {
                            var flinkin_key = getChannelNameById(key.split("@")[0]);
                            if(!countMsg['flinkin']) countMsg['flinkin'] = {};
                            if(!countMsg['flinkin'][flinkin_key]) countMsg['flinkin'][flinkin_key] = {};
                            countMsg.flinkin[flinkin_key]["success"] = wrapper.count[key];
                        }
                        else if (key.indexOf("flinkout_success") != -1) {
                            var flinkout_key = getChannelNameById(key.split("@")[0]);
                            if(!countMsg['flinkout']) countMsg['flinkout'] = {};
                            if(!countMsg['flinkout'][flinkout_key]) countMsg['flinkout'][flinkout_key] = {};
                            countMsg.flinkout[flinkout_key]["success"] = wrapper.count[key];
                        }
                    }
                    $scope.countMessage = countMsg;

                    // $scope.countMessage = wrapper.count
                    if (read_success.length > 0) {
                        $scope.data.push({
                            key: '读取成功',
                            values: read_success
                        });
                    }
                    if ($scope.channel.type.name == 'parser') {
                        if (operation_in.length > 0) {
                            $scope.data.push({
                                key: channelTypeName + '输入',
                                values: operation_in
                            });
                        }
                        if (operation_success.length > 0) {
                            $scope.data.push({
                                key: channelTypeName + '成功',
                                values: operation_success
                            });
                        }
                        if (operation_fail.length > 0) {
                            $scope.data.push({
                                key: channelTypeName + '失败',
                                values: operation_fail
                            });
                        }
                        if (operation_ignore.length > 0) {
                            $scope.data.push({
                                key: channelTypeName + '忽略',
                                values: operation_ignore
                            });
                        }
                    }

                    for (key in write_success) {
                        if (write_success[key].length > 0) {
                            $scope.data.push({
                                key: key + '-输出成功',
                                values: write_success[key]
                            });
                        }
                    }
                    for (key in flink_in) {
                        if (flink_in[key].length > 0) {
                            $scope.data.push({
                                key: key + '-分析输入',
                                values: flink_in[key]
                            });
                        }
                    }
                    for (key in flink_out) {
                        if (flink_out[key].length > 0) {
                            $scope.data.push({
                                key: key + '-分析输出',
                                values: flink_out[key]
                            });
                        }
                    }
                }
            });
        });
    }

    $scope.metrics = function (id) {
        let now = new Date();
        let startTime = d3_format(new Date(now.getTime() - days));
        let endTime = d3_format(now);
        $scope.metric.startTime = startTime;
        $scope.metric.endTime = endTime;
        $scope.metric.dsId = id;
        metricsOne(id, startTime, endTime);
    };

    $scope.udMetrics = function () {
        let startTime = d3_format.parse($scope.metric.startTime);
        let endTime = d3_format.parse($scope.metric.endTime);
        if (startTime > endTime) {
            $scope.options.subtitle.html = '<span style=\'color:rgba(255,0,0,0)\'>时间选择错误：开始时间晚于结束时间！</span>';
            $scope.message = {
                message: "时间选择错误：开始时间晚于结束时间！"
            }
            return;
        } else if ((startTime.getTime() + 1000 * 60 * 60 * 24 * 7) < endTime.getTime()) {
            $scope.options.subtitle.html = '<span style=\'color:rgba(255,0,0,0)\'>时间选择错误：只提供7日内计量信息查询！</span>';
            $scope.message = {
                message: "时间选择错误：只提供7日内计量信息查询"
            }
            return;
        } else {
            $scope.message = ""
        }
        metricsOne($scope.metric.dsId, $scope.metric.startTime, $scope.metric.endTime);
    };

    //重置表单
    $scope.resetForm = function () {
        $scope.channel = defaultChannel;
        Util.resetFormValidateState($scope.ds_form);
    };
    $scope.resetForm();
    //保存
    $scope.save = function (formValid) {
        if (!formValid) {
            return false;
        }
        let dataForm = angular.copy($scope.channel);
        dataForm.writers = setForSave(dataForm.writers);
        //console.log('dataForm='+JSON.stringify(dataForm));
        Channel.save(dataForm, function (rt) {
            $scope.message = rt;
            if ($scope.message.status == '200') {
                $scope.reload();
                $scope.show();
                $('.hla-widget-add-table').slideUp();
            }
        });
    };


    $scope.uploadFile = function () {
        delete $scope.message;
        let $edit_table = $('.hla-widget-add-table');
        let $search_table = $('.hla-widget-search-table');
        let upload_table = $('.hla-widget-upload-table');
        let file = $scope.myFile;
        let reader = new FileReader();
        reader.onload = (function (file) {
            return function (e) {
                $scope.channel = angular.fromJson(this.result);
                delete $scope.channel.id;//删除数据源id
                let list = $scope.collectors.filter(function (collector) {
                    return collector.id == $scope.channel.collectorId;

                });
                if (list.length == 0) {
                    delete $scope.channel.collectorId;
                }
                delete $scope.channel.status;
                //  delete $scope.channel.id;
                delete $scope.channel.datatime;
                setWriterForViewer(angular.copy($scope.channel.writers));
                $edit_table.slideDown();
                $search_table.slideUp();
                upload_table.slideUp();
                delete $scope.myFile;
                $scope.$apply();

            };
        })();
        try {
            reader.readAsText(file);
        }
        catch (e) {
            $scope.message = {
                status: '500',
                message: '数据解析出错,你检查你的数据文件是否正确!'
            };

        }

    };

    function PropertiesWithType() {
        let reg = /^(true)|(false)|\d+$/;
        for (let item in $scope.channel.properties) {
            let v = $scope.channel.properties[item];
            if (v && reg.test(v)) {
                $scope.channel.properties[item] = eval(v)
            }
        }
    }

    $scope.show = function (clazz, id) {

        let $edit_table = $('.hla-widget-add-table');
        let $search_table = $('.hla-widget-search-table');
        let upload_table = $('.hla-widget-upload-table');
        let metric_table = $('.hla-widget-metric-table');
        Util.resetFormValidateState($scope.ds_form);
        switch (clazz) {
            case 'upload':
                delete  $scope.message;
                $edit_table.slideUp();
                $search_table.slideUp();
                metric_table.slideUp();
                upload_table.slideToggle();
                break;

            case 'search':
                delete  $scope.message;

                $edit_table.slideUp();
                $search_table.slideToggle();
                metric_table.slideUp();
                upload_table.slideUp();
                break;
            case 'add':
                delete  $scope.message;
                $edit_table.slideToggle();
                $search_table.slideUp();
                metric_table.slideUp();
                upload_table.slideUp();
                $scope.resetForm();
                break;
            case 'metric-parser':
                delete  $scope.message;
                $scope.channel.type.name = 'parser'
                $scope.metrics(id);
                $edit_table.slideUp();
                $search_table.slideUp();
                metric_table.slideDown();
                upload_table.slideUp();

                break;
            case 'metric-analyzer':
                delete  $scope.message;
                $scope.channel.type.name = 'analyzer'
                $scope.metrics(id);
                $edit_table.slideUp();
                $search_table.slideUp();
                metric_table.slideDown();
                upload_table.slideUp();

                break;
            case 'edit':
                delete  $scope.message;
                Channel.get({id: id}, function (ds) {
                    $scope.channel = ds;
                    //console.log('$scope.channel='+JSON.stringify($scope.channel));
                    if ($scope.channel.type.name === "parser") {
                        if ($scope.datasources.filter(function (item) {
                                return item.id === $scope.channel.datasource;
                            }).length === 0) {
                            delete $scope.channel.datasource
                        }
                    } else {
                        $scope.channels = DataSource.byTypeWithExclude(
                            {
                                type: 'channel',
                                exclude: id
                            });
                        // if ($scope.channels.filter(function (item) {
                        //         return item.id === $scope.channel.datasource;
                        //     }).length === 0) {
                        //     delete $scope.channel.datasource
                        // }
                    }


                    if ($scope.collectors.filter(function (item) {
                            return item.id === $scope.channel.collector;
                        }).length === 0) {
                        delete $scope.channel.collector
                    }
                    if ($scope.resolvers.filter(function (item) {
                            return item.id === $scope.channel.parser;
                        }).length === 0 && $scope.analyzers.filter(function (item) {
                            return item.id === $scope.channel.parser;
                        }).length === 0) {
                        delete $scope.channel.parser
                    }
                    setWriterForViewer(angular.copy(ds.writers));
                    PropertiesWithType();
                    upload_table.slideUp();
                    $edit_table.slideDown();
                    metric_table.slideUp();
                    $search_table.slideUp();
                });

                break;
            case 'copy':
                delete  $scope.message;
                Channel.get({id: id}, function (ds) {
                    $scope.channel = ds;
                    console.log($scope.channel.properties.collector_info);
                    console.log($scope.channel.properties.receive_time);

                    $scope.channel.name = $scope.channel.name + '_copy';
                    delete $scope.channel.id;
                    delete $scope.channel.status;
                    delete $scope.channel.datatime;
                    setWriterForViewer(angular.copy(ds.writers));
                    PropertiesWithType();
                    $edit_table.slideDown();
                    $search_table.slideUp();
                    metric_table.slideUp();
                    upload_table.slideUp();
                });

                break;
            default:
                $edit_table.slideUp();
                $search_table.slideUp();
                metric_table.slideUp();
                upload_table.slideUp();
                break;
        }

    };
    $scope.operate = function (opt, id) {
        Channel.operate({opt: opt, id: id}, function (rt) {
            //console.log('rt='+JSON.stringify(rt))
            $scope.message = rt;
            $scope.reload();
        }, function (error) {
            var content = ""
            if (error.status == 500) {
                content = error.data;
            }
            else if (error.status == -1) {
                content = "Connect to server has timed out, please try again later.";
            }
            else {
                content = "Others exception."
            }
            var message = {
                message: content
            }
            $scope.message = message;
        });

    };

    //删除按钮
    $scope.delete = function (id) {
        $scope.deleteId = id;
        $confirmModal.modal('show');
    };
    //删除确定
    $scope.confirm_yes = function () {
        Channel.delete({id: $scope.deleteId}, function (rt) {
            $scope.message = rt;
            $scope.reload();
        });
        $confirmModal.modal('hide');
    };
    $confirmModal.find('.op_yes').off().click(function () {
        $scope.confirm_yes();
    });
    $scope.reload = function () {
        $scope.list = Channel.get($scope.page, function (pages) {
            if (pages.result.length > 0) {
                //console.log('pages.result='+JSON.stringify(pages.result));
                pages.result = pages.result.map(function (channel) {
                    return channel;
                });
                $scope.page['count'] = pages['totalCount'];
                $scope.page['limit'] = pages['limit'];
                $scope.page['total'] = pages['totalPage'];
            }
            else {
                if(pages['currentPage']>1){
                    $scope.page['current'] = pages['currentPage']-1;
                    $scope.page['count'] = pages['totalCount'];
                    $scope.page['start'] =($scope.page['current'] - 1) * $scope.page['limit'];
                }
                else{
                    $scope.page['current']=1;
                    $scope.page['count']=0;
                    $scope.page['start']=0;
                }
                pages.result = [];
            }
        });
    };
    $scope.addHostPort = function () {
        let listens = $scope.channel.data.hostPorts;
        if (!listens) {
            listens = $scope.channel.data.hostPorts = [];
        }
        listens.push(['', 9200]);
    };
    $scope.removeHostPort = function (index) {
        $scope.channel.data.hostPorts.splice(index, 1);
    };

    $scope.addHostEncoding = function () {
        let listens = $scope.channel.data.listens;
        if (!listens) {
            listens = $scope.channel.data.listens = [];
        }
        listens.push(['', '']);
    };
    $scope.removeHostEncoding = function (index) {
        $scope.channel.data.listens.splice(index, 1);
    };

    //修复数据存储checkbox选中状态
    function setWriterForViewer(writers) {
        if ($scope.writers.$resolved) {
            let tempWriters = new Array($scope.writers.length);
            if (writers) {


                for (let i = 0, j = writers.length; i < j; i++) {
                    let writer = writers[i];
                    if (writer) {
                        let index = getIndex(writer);
                        if (index.length == 2) {
                            tempWriters[index[0]] = {id: index[1].id, name: index[1].name};
                        }
                    }
                }
            }
            $scope.channel.writers = tempWriters;
        }

        function getIndex(id) {
            let data = [];

            for (let i = 0, j = $scope.writers.length; i < j; i++) {
                if ($scope.writers[i].id == id) {
                    data = [i, $scope.writers[i]];
                    break;
                }
            }
            return data;
        }
    }


    //移除数据存储null值
    function setForSave(writers) {
        return (writers || []).filter(function (writer) {
            return writer != null && writer != undefined;
        }).map(function (writer) {
            console.log(writer);
            return writer.id;
        });
    }

    $scope.changeDBType = function () {
        let type = $scope.channel.data.protocol;
        let port = null;
        switch (type) {
            case 'mysql':
                port = 3306;
                break;
            case 'oracle':
                port = 1521;
                break;
            case 'sqlserver':
                port = 1433;
                break;
            case 'DB2':
                port = 50000;
                break;
            case 'sybase':
                port = 5000;
                break;
            case 'postgresql':
                port = 5432;
                break;
            case 'sqlite':
                port = 0;
                break;
            case 'derby':
                port = 1527;
                break;
        }
        $scope.channel.data.port = port;
    };
    $scope.downloadFile = function (itemId) {
//        console.log(sqlItemId);
        Channel.download({id: itemId}, function (response) {
            let fileName = '数据通道配置';
            fileName = decodeURI(fileName);
            let url = URL.createObjectURL(new Blob([response.data]));
            let a = document.createElement('a');
            document.body.appendChild(a); //此处增加了将创建的添加到body当中
            a.href = url;
            a.download = fileName + '-' + itemId + '.json';
            a.target = '_blank';
            a.click();
            a.remove(); //将a标签移除
        }, function (response) {
//            console.log(response);
        });
    }//
});
