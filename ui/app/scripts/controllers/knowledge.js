'use strict';

app.controller('KnowledgeController', function ($scope, $rootScope, $location, Knowledge, Util, DataSource, Metrics, Collector, Parser) {
    $rootScope.$path = $location.path.bind($location);
    $scope.encodings = ['UTF-8', 'GBK', 'GB18030', 'GB2312', 'BIG5', 'UNICODE', 'ASCII', 'ISO-8859-1'];
    const $confirmModal = $('#confirmModal');
    $scope.countMessage = '采集的总量：未知';
    const d3_format = d3.time.format('%Y-%m-%d %H:%M:%S');
    let days = 1 * 60 * 60 * 1000;
    const knowledgeDefault = {
        name: '',
        collector: '',
        datasource: '',
        parser: ''
    };

    //采集器
    $scope.collectors = Collector.query();
    //数据源
    $scope.datasources = DataSource.query();
    //数据通道
    $scope.knowledges = Knowledge.query();

    //解析规则
    $scope.resolvers = Parser.query();
    $scope.resolverList = $scope.resolvers;

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

    //重置表单
    $scope.resetForm = function () {
        $scope.knowledge = angular.copy(knowledgeDefault);
        Util.resetFormValidateState($scope.ds_form);
    };
    $scope.resetForm();
    //保存
    $scope.save = function (formValid) {
        if (!formValid) return false;
        let dataForm = angular.copy($scope.knowledge);
        Knowledge.save(dataForm, function (rt) {
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
                $scope.knowledge = angular.fromJson(this.result);
                delete $scope.knowledge.id;//删除知识库的id
                //删除不存在的采集器
                let collectorList = $scope.collectors.filter(function (collector) {
                    return collector.id == $scope.knowledge.collector;

                });
                if (collectorList.length === 0) {
                    delete $scope.knowledge.collector;
                }
                //删除不存在的数据源
                let datasourceList = $scope.datasources.filter(function (datasource) {
                    return datasource.id === $scope.knowledge.datasource;

                });
                if (datasourceList.length === 0) {
                    delete $scope.knowledge.datasource;
                }
                //删除不存在的解析规则
                let parserList = $scope.resolvers.filter(function (resolvers) {
                    return resolvers.id === $scope.knowledge.parser;

                });
                if (parserList.length === 0) {
                    delete $scope.knowledge.parser;
                }

                delete $scope.knowledge.status;
                //  delete $scope.knowledge.id;
                $edit_table.slideDown();
                $search_table.slideUp();
                upload_table.slideUp();
                delete $scope.myFile;
                $scope.$apply();

            };
        })();
        try {
            reader.readAsText(file);
        } catch (e) {
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
        Util.resetFormValidateState($scope.ds_form);
        switch (clazz) {
            case 'upload':
                delete  $scope.message;
                $edit_table.slideUp();
                $search_table.slideUp();
                upload_table.slideToggle();
                break;

            case 'search':
                delete  $scope.message;
                $edit_table.slideUp();
                $search_table.slideToggle();
                upload_table.slideUp();
                break;
            case 'add':
                delete  $scope.message;
                $edit_table.slideToggle();
                $search_table.slideUp();
                upload_table.slideUp();
                $scope.resetForm();
                break;
            case 'edit':
                delete  $scope.message;
                Knowledge.get({id: id}, function (ds) {
                    $scope.knowledge = ds;
                    upload_table.slideUp();
                    $edit_table.slideDown();
                    $search_table.slideUp();
                });

                break;
            case 'copy':
                delete  $scope.message;
                //console.log(id);
                Knowledge.get({id: id}, function (ds) {
                    $scope.knowledge = ds;
                    $scope.knowledge.name = $scope.knowledge.name + '_copy';
                    delete $scope.knowledge.id;
                    delete $scope.knowledge.status;
                    delete $scope.knowledge.datatime;
                    $edit_table.slideDown();
                    $search_table.slideUp();
                    upload_table.slideUp();
                });

                break;
            default:
                $edit_table.slideUp();
                $search_table.slideUp();
                upload_table.slideUp();
                break;
        }

    };
    $scope.operate = function (opt, id) {
        Knowledge.operate({opt: opt, id: id}, function (rt) {
            $scope.message = rt;
            //console.log('rt='+JSON.stringify(rt))
            $scope.reload();
        });

    };

    //删除按钮
    $scope.delete = function (id) {
        $scope.deleteId = id;
        $confirmModal.modal('show');
    };
    //删除确定
    $scope.confirm_yes = function () {
        Knowledge.delete({id: $scope.deleteId}, function (rt) {
            $scope.message = rt;
            $scope.reload();
        });
        $confirmModal.modal('hide');
    };
    $confirmModal.find('.op_yes').off().click(function () {
        $scope.confirm_yes();
    });
    $scope.reload = function () {
        $scope.list = Knowledge.get($scope.page, function (pages) {
            if (pages.result.length > 0) {
                //console.log('pages.result='+JSON.stringify(pages.result));
                pages.result = pages.result.map(function (knowledge) {
                    return knowledge;
                });
                $scope.page['count'] = $scope.list['totalCount'];
                $scope.page['limit'] = $scope.list['limit'];
                $scope.page['total'] = $scope.list['totalPage'];
            } else {
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
        let listens = $scope.knowledge.data.hostPorts;
        if (!listens) {
            listens = $scope.knowledge.data.hostPorts = [];
        }
        listens.push(['', 9200]);
    };
    $scope.removeHostPort = function (index) {
        $scope.knowledge.data.hostPorts.splice(index, 1);
    };

    $scope.addHostEncoding = function () {
        let listens = $scope.knowledge.data.listens;
        if (!listens) {
            listens = $scope.knowledge.data.listens = [];
        }
        listens.push(['', '']);
    };
    $scope.removeHostEncoding = function (index) {
        $scope.knowledge.data.listens.splice(index, 1);
    };


    $scope.changeDBType = function () {
        let type = $scope.knowledge.data.protocol;
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
        $scope.knowledge.data.port = port;
    };
    $scope.downloadFile = function (itemId) {
//        console.log(sqlItemId);
        Knowledge.download({id: itemId}, function (response) {
            let fileName = '知识库配置';
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
