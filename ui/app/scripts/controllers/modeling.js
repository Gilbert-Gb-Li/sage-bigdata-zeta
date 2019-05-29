'use strict';

app.controller('ModelingController', function ($scope, $rootScope, $location, Modeling, Analyzer, Util, DataSource, Metrics, Collector, Parser, Writer, Channel) {
    $rootScope.$path = $location.path.bind($location);
    $scope.encodings = ['UTF-8', 'GBK', 'GB18030', 'GB2312', 'BIG5', 'UNICODE', 'ASCII', 'ISO-8859-1'];
    const $confirmModal = $('#confirmModal');
    $scope.countMessage = '采集的总量：未知';
    const d3_format = d3.time.format('%Y-%m-%d %H:%M:%S');
    let days = 1 * 60 * 60 * 1000;

    //采集器
    $scope.collectors = Collector.query();
    //数据源
    /*$scope.datasources = DataSource.query();*/
    //数据通道列表
    $scope.channels = DataSource.byType({type: "channel"});

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

    //数据存储
    $scope.sinks = Writer.query();

    $scope.analyzeSel = function () {
        $scope.analyzers.find(function(item){
            if (item.id === $scope.modeling.analyzer) {
                if (item.data.name === 'sql') {
                    $scope.modeling.type.type = 'analyzer';
                    $scope.modeling.typeDisable = true;

                }
                else {
                    //$scope.modeling.type = 'analyzer';
                    $scope.modeling.typeDisable = false;

                }
                return true
            }
            else {
                return false
            }
        })
    };

    $scope.usableCheck = function () {
        let data = angular.copy($scope.modeling);
        data.sinks = setForSave(data.sinks);
        Modeling.check(data, function (rt) {
            $scope.rt = rt;
        });
    };

    //重置表单
    $scope.resetForm = function () {
        $scope.modeling = {sinks: [], type: {name: "analyzer"}};
        Util.resetFormValidateState($scope.ds_form);
    };
    $scope.resetForm();
    //保存
    $scope.save = function (formValid) {
        if (!formValid) {
            return false;
        }
        let dataForm = angular.copy($scope.modeling);
        dataForm.sinks = setForSave(dataForm.sinks);
        //console.log('dataForm='+JSON.stringify(dataForm));
        Modeling.save(dataForm, function (rt) {
            $scope.message = rt;
            if ($scope.message.status === '200') {
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
                $scope.modeling = angular.fromJson(this.result);
                delete $scope.modeling.id;//删除数据源id

                let list = $scope.collectors.filter(function (collector) {
                    return collector.id === $scope.modeling.worker;

                });
                if (list.length === 0) {
                    delete $scope.modeling.worker;
                }

                /*//删除不存在的数据源
                 let datasourceList = $scope.datasources.filter(function (datasource) {
                 return datasource.id == $scope.modeling.channel;

                 });
                 if (datasourceList.length == 0) {
                 delete $scope.modeling.channel;
                 }*/

                //删除不存在的解析规则
                let analyzerList = $scope.analyzers.filter(function (analyzer) {
                    return analyzer.id === $scope.modeling.analyzer;

                });
                if (analyzerList.length === 0) {
                    delete $scope.modeling.analyzer;
                }

                delete $scope.modeling.status;
                //  delete $scope.modeling.id;
                delete $scope.modeling.datatime;
                setWriterForViewer(angular.copy($scope.modeling.sinks));
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
            case 'metric':
                delete  $scope.message;
                $scope.metrics(id);
                $edit_table.slideUp();
                $search_table.slideUp();
                metric_table.slideDown();
                upload_table.slideUp();

                break;
            case 'edit':
                delete  $scope.message;
                Modeling.get({id: id}, function (ds) {
                    $scope.modeling = ds;
                    // console.log('$scope.modeling=' + JSON.stringify($scope.modeling));
                    setWriterForViewer(angular.copy(ds.sinks));
                    $scope.analyzeSel();
                    /*
                     $scope.querySourceType($scope.modeling.channel, function () {
                     propertiesWithType();
                     upload_table.slideUp();
                     $edit_table.slideDown();
                     metric_table.slideUp();
                     $search_table.slideUp();
                     })*/
                    propertiesWithType();
                    upload_table.slideUp();
                    $edit_table.slideDown();
                    metric_table.slideUp();
                    $search_table.slideUp();
                });

                break;
            case 'copy':
                delete  $scope.message;
                Modeling.get({id: id}, function (ds) {

                    $scope.modeling = ds;
                    $scope.modeling.name = $scope.modeling.name + '_copy';
                    delete $scope.modeling.id;
                    if ($scope.modeling.type && $scope.modeling.type.jobId) {
                        delete $scope.modeling.type.jobId;
                    }
                    delete $scope.modeling.status;
                    delete $scope.modeling.datatime;
                    setWriterForViewer(angular.copy(ds.sinks));
                    propertiesWithType();
                    $edit_table.slideDown();
                    $search_table.slideUp();
                    metric_table.slideUp();
                    upload_table.slideUp();
                });

                break;
            case "connectTest":
                $search_table.slideUp();
                metric_table.slideUp();
                upload_table.slideUp();
                break;
            default:
                $edit_table.slideUp();
                $search_table.slideUp();
                metric_table.slideUp();
                upload_table.slideUp();
                break;
        }

    };
    $scope.connectTest = function (valid) {
        var workerId = $scope.modeling.worker;
        var url = $scope.modeling.type.cluster;
        if (valid) {
            angular.element("#connect_test").html('<i class="fa fa-spinner fa-spin"></i>');
            Modeling.connectTest({opt: 'check', workerId: workerId, url: url}, function (rt) {
                $scope.message = rt;
                angular.element("#connect_test").html('测试连接');
            }, function (error) {
                var content = "Connect to server has timed out, please try again later.";
                var message = {
                    message: content
                };
                $scope.message = message;
                angular.element("#connect_test").html('测试连接');
            });
        }
    };
    $scope.operate = function (opt, id) {
        Modeling.operate({opt: opt, id: id}, function (rt) {
            $scope.message = rt;
            $scope.reload();
        }, function (error) {
            var content = "Connect to server has timed out, please try again later.";
            var message = {
                message: content
            };
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
        Modeling.delete({id: $scope.deleteId}, function (rt) {
            $scope.message = rt;
            $scope.reload();
        });
        $confirmModal.modal('hide');
    };
    $confirmModal.find('.op_yes').off().click(function () {
        $scope.confirm_yes();
    });
    $scope.reload = function () {
        $scope.list = Modeling.get($scope.page, function (pages) {
            if (pages.result.length > 0) {
                pages.result = pages.result.map(function (modeling) {
                    return modeling;
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
        let listens = $scope.modeling.data.hostPorts;
        if (!listens) {
            listens = $scope.modeling.data.hostPorts = [];
        }
        listens.push(['', 9200]);
    };
    $scope.removeHostPort = function (index) {
        $scope.modeling.data.hostPorts.splice(index, 1);
    };

    $scope.addHostEncoding = function () {
        let listens = $scope.modeling.data.listens;
        if (!listens) {
            listens = $scope.modeling.data.listens = [];
        }
        listens.push(['', '']);
    };
    $scope.removeHostEncoding = function (index) {
        $scope.modeling.data.listens.splice(index, 1);
    };

    //修复数据存储checkbox选中状态
    function setWriterForViewer(sinks) {
        if ($scope.sinks.$resolved) {
            let tempWriters = new Array($scope.sinks.length);
            if (sinks) {


                for (let i = 0, j = sinks.length; i < j; i++) {
                    let writer = sinks[i];
                    if (writer) {
                        let index = getIndex(writer);
                        if (index.length === 2) {
                            tempWriters[index[0]] = {id: index[1].id, name: index[1].name};
                        }
                    }
                }
            }
            $scope.modeling.sinks = tempWriters;
        }

        function getIndex(id) {
            let data = [];

            for (let i = 0, j = $scope.sinks.length; i < j; i++) {
                if ($scope.sinks[i].id === id) {
                    data = [i, $scope.sinks[i]];
                    break;
                }
            }
            return data;
        }
    }


    //移除数据存储null值
    function setForSave(sinks) {
        return (sinks || []).filter(function (sink) {
            return sink !== null && sink !== undefined;
        }).map(function (sink) {
            return sink.id;
        });
    }

    $scope.downloadFile = function (itemId) {
        Modeling.download({id: itemId}, function (response) {
            let fileName = '数据建模配置';
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
    };

    // 查询数据源类型
    $scope.querySourceType = function (sourceId, callback) {
        if ((typeof sourceId !== "undefined") && (typeof sourceId.valueOf() === "string") && (sourceId.length > 0)) {
            DataSource.get({id: sourceId}, function (ds) {
                $scope.modeling.sourceType = ds.data.name;
                // console.log('ds=' + JSON.stringify(ds));
                // console.log('$scope.modeling.sourceType=' + $scope.modeling.sourceType);
                callback();
            });
        }
        else {
            callback();
        }
    };

    // 数据源选择事件
    $scope.changeDataSource = function (channelId) {
        $scope.querySourceType(channelId, function () {
            /*$('.hla-widget-upload-table').slideUp();
             $('.hla-widget-add-table').slideDown();
             $('.hla-widget-metric-table').slideUp();
             $('.hla-widget-search-table').slideUp();*/
        });
    };

    function propertiesWithType() {
        let reg = /^(true)|(false)|\d+$/;
        for (let item in $scope.modeling.properties) {
            let v = $scope.modeling.properties[item];
            if (v && reg.test(v)) {
                $scope.modeling.properties[item] = eval(v)
            }
        }
    }

    $scope.typeChange = function () {
        //分析规则
        // console.log($scope.modeling.type+"====================" +$scope.type)
        Analyzer.queryByType({type: $scope.modeling.type.name}, function (rt) {
            $scope.analyzers = rt;
        })
    }
});
