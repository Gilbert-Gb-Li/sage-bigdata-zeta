'use strict';

app.controller('DataSourceController', function ($scope, $rootScope, $location, Util, DataSource, Collector, Parser,Channel) {
    $rootScope.$path = $location.path.bind($location);
    $scope.encodings = ['UTF-8', 'GBK', 'GB18030', 'GB2312', 'BIG5', 'UNICODE', 'ASCII', 'ISO-8859-1'];
    $scope.isHaveThead = true;
    const $confirmModal = $('#confirmModal');
    let days = 1 * 60 * 60 * 1000;
    $scope.previewData = {};
    //采集器
    $scope.collectors = Collector.query();
    //解析规则
    $scope.parsers = Parser.query();

    //数据通道列表
    $scope.channels = DataSource.byType({type: "channel-without-table"});
    //数据通道列表
    // $scope.tables = DataSource.byType({type: "table"});

    const dataSourceDefault = {
        properties: {
            polling_ms: '1000',
            timeout_ms: '100',
            connectTimeout: '300'
        },
        data: {
            name: 'file',
            contentType: {
                name: 'txt'
            },
            position: 'END',
            skipLine: 0,
            protocol: {
                name: 'net',
                protocol: {
                    name: 'udp'
                }
            },
            cache: 1000,
            codec: {
                name: 'line'
            },
            metadata: {
                timestamp: '@timestamp'
            },
            authorization: false,
            properties: {
                connectTimeout: '300'
            },
            table:{
                timeCharacteristic:{
                    name:'processing'
                }
            },
            on:{
                window:{
                    name:'tumbling',
                    window:10000,
                    gap:10000,
                    sliding:1000,
                    timeCharacteristic:{
                        name:'processing'
                    }
                },
                first:{
                    timeCharacteristic:{
                        name:'processing'
                    }
                },
                second:{
                    timeCharacteristic:{
                        name:'processing'
                    }
                }
            }
        }
    };


    $scope.changeTimeCharacteristic = function () {

        /*$scope.datasource.data.on.first = $scope.datasource.data.on.second = {
            'timeCharacteristic': {
                'name': $scope.datasource.data.on.window.timeCharacteristic.name
            }
        };*/

        if ($scope.datasource.data.on.first.timeCharacteristic.name == "event" || $scope.datasource.data.on.second.timeCharacteristic.name == "event") {
            $scope.datasource.data.on.window.timeCharacteristic.name = "event"
        } else {
            $scope.datasource.data.on.window.timeCharacteristic.name = "processing"
        }

        /*if (!$scope.datasource.data.on.first.timeCharacteristic) {
            $scope.datasource.data.on.first.timeCharacteristic = {
                'name': $scope.datasource.data.on.window.timeCharacteristic.name
            }
        } else {
            $scope.datasource.data.on.first.timeCharacteristic.name =
                $scope.datasource.data.on.window.timeCharacteristic.name
        }

        if (!$scope.datasource.data.on.second.timeCharacteristic) {
            $scope.datasource.data.on.second.timeCharacteristic = {
                'name': $scope.datasource.data.on.window.timeCharacteristic.name
            }
        } else {
            $scope.datasource.data.on.second.timeCharacteristic.name =
                $scope.datasource.data.on.window.timeCharacteristic.name
        }*/

    };


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
            html: '采集的总量：未知',
            enable: true,
            css: {
                'text-align': 'justify',
                'margin': '10px 13px 0px 7px'
            }
        }
    };


    $scope.preview = function (valid) {
        console.log($scope.datasource);
        if (!valid) {
            return false;
        }
        DataSource.preview($scope.datasource, function (data) {
            $scope.previewData = data;
            $scope.showPreview = true;
        });
    };

    //重置表单
    $scope.resetForm = function () {
        $scope.datasource = angular.copy(dataSourceDefault);
        Util.resetFormValidateState($scope.ds_form);
    };
    $scope.resetForm();
    //保存
    $scope.saveDataSource = function (formValid) {
        if (!formValid) {
            return false;
        }
        let dataForm = angular.copy($scope.datasource);
        delete dataForm.data.uri;
        // if( dataForm.data.name == "es2" || dataForm.data.name == "es5" ) {
        //     dataForm.data.name = "es"
        // }
        DataSource.save(dataForm, function (rt) {
            $scope.message = rt;
            if ($scope.message.status == '200') {
                $scope.reload();
                $scope.show();
                $('.hla-widget-add-table').slideUp();
                $scope.reload();
            }

        });

    };


    $scope.deleteHeader = function () {
        let contentType = $scope.datasource.data.contentType && $scope.datasource.data.contentType;
        if (contentType.isHaveThead &&
            (contentType.isHaveThead === true ||
                contentType.isHaveThead === "true")) {
            delete contentType.header;
        }
        let pathContentType = $scope.datasource.data.path && $scope.datasource.data.path.contentType && $scope.datasource.data.path.contentType;
        if (pathContentType.isHaveThead &&
            (pathContentType.isHaveThead === true ||
                pathContentType.isHaveThead === "true")) {
            delete pathContentType.header;
        }
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
                $scope.datasource = angular.fromJson(this.result);
                delete $scope.datasource.id; //删除数据源id
                delete $scope.datasource.status;
                //  delete $scope.datasource.id;
                delete $scope.datasource.datatime;
                // 如果上传的数据源配置中包含的CollectorID不存在，删除该属性
                if (!$scope.collectors.filter(r => r.id === $scope.datasource.collector).shift()) {
                    delete $scope.datasource.collector;
                }
                // 判断上传的“数据转表”数据源中使用的数据通道是否存在，如果不存在删除该属性
                if ($scope.datasource.data.name === "single-table") {
                    if (!$scope.channels.filter(r => r.id === $scope.datasource.data.channel.id).shift()) {
                        delete $scope.datasource.data.channel.id;
                    }
                }
                // 判断上传的“通道组合”数据源中使用的数据通道是否存在，如果不存在删除该属性
                if ($scope.datasource.data.name === "tuple") {
                    if (!$scope.channels.filter(r => r.id === $scope.datasource.data.first.id).shift()) {
                        delete $scope.datasource.data.first.id;
                    }
                    if (!$scope.channels.filter(r => r.id === $scope.datasource.data.second.id).shift()) {
                        delete $scope.datasource.data.second.id;
                    }
                }


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
    // 子数据通道是否是SQL聚合分析
    $scope.tupleChannelForSql = {
        first: false,
        second: false
    };
    $scope.checkChildChannelForTupleChannelOfSql = function (table) {
        if ($scope.datasource.data[table].id) {
            var channelSelected = $scope.channels.filter(r => r.id === $scope.datasource.data[table].id).shift();
            if (channelSelected && channelSelected.data && channelSelected.data.name === "tuple" && channelSelected.data.on.name === "sql") {
                $scope.tupleChannelForSql[table] = true;
            }
            else {
                $scope.tupleChannelForSql[table] = false;
            }
        }
    };
    $scope.show = function (clazz, id, name) {
        var type = 'channel';
        let $edit_table = $('.hla-widget-add-table');
        let $search_table = $('.hla-widget-search-table');
        let upload_table = $('.hla-widget-upload-table');
        let metric_table = $('.hla-widget-metric-table');
        Util.resetFormValidateState($scope.ds_form);
        switch (clazz) {
            case 'upload':
                delete $scope.message;
                $edit_table.slideUp();
                $search_table.slideUp();
                metric_table.slideUp();
                upload_table.slideToggle();
                $('.fileinput-remove').trigger('click');
                break;
            case 'search':
                delete $scope.message;
                $edit_table.slideUp();
                $search_table.slideToggle();
                metric_table.slideUp();
                upload_table.slideUp();
                break;
            case 'add':
                $scope.channels = DataSource.byType(
                    {type: type},
                    function () {
                        delete $scope.message;
                        $scope.previewData = {};
                        $scope.showPreview = false;
                        $edit_table.slideToggle();
                        $search_table.slideUp();
                        metric_table.slideUp();
                        upload_table.slideUp();
                        $scope.resetForm();
                    }
                );
                break;
            case 'edit':
                delete $scope.message;
                $scope.showPreview = false;
                if (name === 'tuple-table'){
                    type = 'table';
                }
                if (name === 'table' || name === 'tuple'){
                    type = 'channel-without-table';
                }
                DataSource.get({
                    id: id
                }, function (ds) {
                    $scope.channels = DataSource.byTypeWithExclude(
                        {
                            type: type,
                            exclude: id
                        },
                        function () {
                            $scope.datasource = ds;
                            if ($scope.datasource.data.properties && $scope.datasource.data.properties.fromstart) {
                                $scope.datasource.data.properties.fromstart = $scope.datasource.data.properties.fromstart ? $scope.datasource.data.properties.fromstart == 'true' : false;
                            }
                            upload_table.slideUp();
                            $edit_table.slideDown();
                            metric_table.slideUp();
                            $search_table.slideUp();
                        }
                    );
                });
                break;
            case 'copy':
                delete $scope.message;
                DataSource.get({
                    id: id
                }, function (ds) {
                    $scope.datasource = ds;
                    $scope.previewData = {};
                    $scope.showPreview = false;
                    $scope.datasource.name = $scope.datasource.name + '_copy';
                    /*if ($scope.datasource.data.name === "tuple") {
                     $scope.checkChildChannelForTupleChannelOfSql("first");
                     $scope.checkChildChannelForTupleChannelOfSql("second");
                     }*/
                    delete $scope.datasource.id;
                    delete $scope.datasource.status;
                    delete $scope.datasource.datatime;
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

    //删除按钮
    $scope.delete = function (id) {
        $scope.deleteId = id;
        $confirmModal.modal('show');
    };
    //删除确定
    $scope.confirm_yes = function () {
        let $edit_table = $('.hla-widget-add-table');
        DataSource.delete({
            id: $scope.deleteId
        }, function (rt) {
            $scope.message = rt;
            if ($scope.message.status == '200') {
                $scope.reload();
                $edit_table.slideUp();
            }
        });
        $confirmModal.modal('hide');
    };
    $confirmModal.find('.op_yes').off().click(function () {
        $scope.confirm_yes();
    });
    $scope.reload = function () {
        $scope.list = DataSource.get($scope.page, function (pages) {

            if (pages.result.length > 0) {
                pages.result = pages.result.map(function (item) {
                    return item;
                });
                $scope.page['count'] = pages['totalCount'];
                $scope.page['limit'] = pages['limit'];
                $scope.page['total'] = pages['totalPage'];
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
        let listens = $scope.datasource.data.hostPorts;
        if (!listens) {
            listens = $scope.datasource.data.hostPorts = [];
        }
        listens.push(['', 9200]);
        //listens.push(['', '']);
    };
    $scope.removeHostPort = function (index) {
        $scope.datasource.data.hostPorts.splice(index, 1);
    };

    $scope.addHostEncoding = function () {
        let listens = $scope.datasource.data.listens;
        if (!listens) {
            listens = $scope.datasource.data.listens = [];
        }
        listens.push(['', '']);
    };
    $scope.removeHostEncoding = function (index) {
        $scope.datasource.data.listens.splice(index, 1);
    };

    $scope.changeCodec = function () {
        let codecName = $scope.datasource.data.codec.name;
        switch (codecName) {
            case "multi":
                $scope.datasource.data.codec.inStart = false;
                $scope.datasource.data.codec.maxLineNum = '1000';
        }
    };

    $scope.pathChangeCodec = function () {
        let codecName = $scope.datasource.data.path.codec.name;
        switch (codecName) {
            case "multi":
                $scope.datasource.data.path.codec.inStart = false;
                $scope.datasource.data.path.codec.maxLineNum = '1000';
        }
    };
    $scope.changeDBType = function () {
        let type = $scope.datasource.data.protocol;
        let port = null;
        $scope.drivers = []
        switch (type) {
            case 'mysql':
                port = 3306;
                $scope.datasource.data.driver = "com.mysql.jdbc.Driver"
                break;
            case 'oracle':
                port = 1521;
                $scope.datasource.data.driver = "oracle.jdbc.driver.OracleDriver"
                break;
            case 'sqlserver':
                port = 1433;
                $scope.datasource.data.driver = "com.microsoft.jdbc.sqlserver.SQLServerDriver"
                break;
            case 'DB2':
                $scope.datasource.data.driver = "com.ibm.db2.jcc.DB2Driver"
                port = 50000;
                break;
            case 'sybase':
                port = 5000;
                $scope.datasource.data.driver = "net.sourceforge.jtds.jdbc.Driver"
                break;
            case 'postgresql':
                $scope.datasource.data.driver = "org.postgresql.Driver"
                port = 5432;
                break;
            case 'sqlite':
                $scope.datasource.data.driver = "org.sqlite.JDBC"
                port = 0;
                break;
            case 'derby':
                $scope.datasource.data.driver = "org.apache.derby.jdbc.EmbeddedDriver"
                port = 1527;
                break;
            case 'phoenix':
                $scope.datasource.data.driver = "org.apache.phoenix.jdbc.PhoenixDriver"
                port = 2181;
                break;
        }
        $scope.datasource.data.port = port;
    };
    $scope.changeDataSourceName = function () {
        let name = $scope.datasource.data.name;
        let data = angular.extend(angular.copy(dataSourceDefault.data), {
            name: name
        });
        let type = 'channel';
        $scope.single = 'line';
        if (name === 'file') {
        }
        else if (name === 'net') {
            data.contentType = 'net';
            data.host = '0.0.0.0';
            data.port = 514;
        }
        else if (name === 'ftp' || name === 'hdfs' || name === 'sftp') {
            if (name === 'ftp') {
                data.port = 21;
            }
            else if (name === 'sftp') {
                data.port = 22;
            }
            else if (name === "hdfs") {
                data.port = 9000;
                data.authentication = "NONE";
            }
            data.path = {};
            angular.extend(data.path, {
                contentType: data.contentType,
                name: 'file',
                category: 'other',
                position: data.position,
                skipLine: data.skipLine,
                codec: data.codec
            });
        }
        else if (name === 'es2' || name === 'es5' || name === 'es6') {
            data.index = 'logs_$';
            data.esType = 'logs';
            data.field = '@timestamp';
            data.hostPorts = [];
        }
        else if (name == 'kafka') {
            data.wildcard = 'false';
        }
        else if (name == 'tuple') {
            $scope.resolver = {
                first: {},
                second: {}
            }
            $scope.timefields = {
                first: [],
                second: []
            }
            type = 'channel-without-table';
        }
        else if (name == 'single-table') {
            data.channel = {};
            data.table = {
                timeCharacteristic:{
                    name:'processing',
                    maxOutOfOrderness: 10000
                },
                fields: []
            };
            $scope.resolver = {
                channel: {}
            };
            $scope.timefields = {
                channel: []
            };
            type = 'channel-without-table';
        }
        else if (name == 'tuple-table') {
            type = 'table';
        }
        else {
            data.data = {};
            delete data.contentType;
            delete data.properties;
            delete data.position;
            delete data.skipLine;
            delete data.protocol;
            delete data.cache;
        }
        if (name == 'single-table' || name == 'tuple-table' || name == 'tuple'){
            $scope.channels = DataSource.byType({type: type});
        }
        $scope.datasource.data = data;
    };

    $scope.downloadFile = function (itemId) {
        //        console.log(sqlItemId);
        DataSource.download({
            id: itemId
        }, function (response) {
            let fileName = '数据源配置';
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
    } //

    $scope.changeChannel = function (table) {
        if ($scope.datasource.data[table].id) {
            if (table == "channel") {
                let channel = $scope.channels.filter(r => r.id === $scope.datasource.data[table].id).shift();
                let channelId = channel.id;
                Channel.get({id: channelId}, function (ds) {
                    $scope.resolver[table]= $scope.parsers.filter(r => r.id === ds.parser).shift();
                });
            }
            else {
                /*$scope.checkChildChannelForTupleChannelOfSql(table);*/
                var channelSelected = $scope.channels.filter(r => r.id === $scope.datasource.data[table].id).shift();
                if($scope.datasource.data[table]){
                    $scope.datasource.data[table]['name'] = channelSelected.data.name;
                }
                if($scope.datasource.data.on) {
                    $scope.datasource.data.on[table].parserId = $scope.channels.filter(r => r.id === $scope.datasource.data[table].id).shift().parser;
                    if ($scope.datasource.data.on[table].parserId) {
                        $scope.resolver[table] = $scope.parsers.filter(r => r.id === $scope.datasource.data.on[table].parserId).shift();
                    }
                }
            }
            if ($scope.resolver && $scope.resolver[table] && $scope.resolver[table].properties) {
                $scope.timefields[table] = $scope.resolver[table].properties.filter(r => (r.type === 'datetime' || r.type === 'long'));
            }
        }
    };

    $scope.changeFieldName = function (table, index) {
        let fieldName = "";
        if (table == "channel") {
            fieldName = $scope.datasource.data.table.fields[index][0];
        }
        else {
            fieldName = $scope.datasource.data.table.fields[index][0];
        }
        if ($scope.resolver && $scope.resolver[table] && $scope.resolver[table].properties) {
            let p = $scope.resolver[table].properties.filter(p => p.key === fieldName).shift();
            if (p) {
                if (table == "channel") {
                    $scope.datasource.data.table.fields[index][1] = p.type;
                }
                else {
                    $scope.datasource.data.table.fields[index][1] = p.type;
                }
            }
            /*else {
             $scope.message = {
             status: '300',
             title: '警告！',
             message: "解析规则[" + $scope.resolver.name + "]不存在字段[" + fieldName + "]，请确认是否添加"
             };
             }*/
        }
    };

    /**
     * 构造 字段属性
     */
    $scope.addProperty = function (table) {
        if (table == "channel") {
            if ($scope.datasource.data.table.fields) {
                $scope.datasource.data.table.fields.push(['', 'string']);
            }
            else {
                $scope.datasource.data.table.fields = [];
                $scope.datasource.data.table.fields.push(['', 'string']);
            }
        }
        else {
            if ($scope.datasource.data.on[table].fields) {
                $scope.datasource.data.on[table].fields.push(['', 'string']);
            }
            else {
                $scope.datasource.data.on[table].fields = [];
                $scope.datasource.data.on[table].fields.push(['', 'string']);
            }
            /*if ($scope.datasource.data.on[table].parserId) {
             if ($scope.datasource.data.on[table].fields) {
             $scope.datasource.data.on[table].fields.push(['', 'string']);
             }
             else {
             $scope.datasource.data.on[table].fields = [];
             $scope.datasource.data.on[table].fields.push(['', 'string']);
             }
             }
             else {
             $scope.confirm = {
             title: '提示',
             message: "您未选择解析规则，确认添加字段么？",
             ok: function () {
             $scope.datasource.data.on[table].fields.push(['', 'string']);
             $('#commonConfirmModal').modal('hide');
             },
             cancel: function () {
             $('#commonConfirmModal').modal('hide');
             }
             };
             $('#commonConfirmModal').modal('show');
             }*/
        }
    };
    /**
     * 删除字段属性
     * @param index
     */
    $scope.deleteProperty = function (table, index) {
        if (table == "channel") {
            $scope.datasource.data.table.fields.splice(index, 1);
        }
        else {
            $scope.datasource.data.on[table].fields.splice(index, 1);
        }
    };
    $scope.changeTimeField = function (table) {
        if ($scope.timefields[table]) {
            let timefield = "";
            if (table == "channel") {
                timefield = $scope.timefields[table].filter(t => t.key === $scope.datasource.data.table.timeField).shift();
            }
            else {
                timefield = $scope.timefields[table].filter(t => t.key === $scope.datasource.data.on[table].timeField).shift();
            }
            if (!timefield) {
                $scope.message = {
                    status: '300',
                    title: "警告！",
                    message: "解析规则中不存在您填写的时间字段，请确认字段是否为时间类型或long类型"
                };
            }
        }
    };

    $scope.inputAllTableFields = function () {
        if ($scope.resolver && $scope.resolver.channel && $scope.resolver.channel.properties) {
            $scope.resolver.channel.properties.forEach(
                function (p, i) {
                    $scope.addProperty('channel');
                    $scope.datasource.data.table.fields[i][0] = p.key;
                    $scope.datasource.data.table.fields[i][1] = p.type;
                }
            )
        }
    };
    let timeField = null;
});
