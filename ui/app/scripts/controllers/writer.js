'use strict';

app.controller('WriterController', function ($scope, $rootScope, $location, Util, Writer, Collector) {
    $rootScope.$path = $location.path.bind($location);
    $scope.encodings = ['UTF-8', 'GBK', 'GB18030', 'GB2312', 'BIG5', 'UNICODE', 'ASCII', 'ISO-8859-1'];
    const $confirmModal = $('#confirmModal');
    const WriterDefault = {
        data: {
            indexType:'data',
            name: 'es5',
            number_of_shards: 5,
            number_of_replicas: 0,
            cache: 1000,
            date_detection: true,
            enable_size: false,
            persisRef: {
                name: 'none'
            },
            protocol: {
                name: 'udp',
            },
            contentType: {
                name: 'json',
                contentType: {
                    name: 'json'
                }
            },
            authentication: 'NONE',
            properties: {
                connectTimeout: '300'
            }
        }
    };
    //采集器
    $scope.collectors = Collector.query();
    $scope.writers = Writer.query();
    //重置表单
    $scope.resetForm = function () {
        $scope.writer = angular.copy(WriterDefault);
        Util.resetFormValidateState($scope.ds_form);
    };

    $scope.resetForm();

    $scope.changeName = function () {
        switch ($scope.writer.data.name) {
            case 'hdfs':
                $scope.writer.data.authentication = 'NONE';
            case 'ftp':
            case 'sftp':

                $scope.writer.data.path = angular.copy({
                    name: 'file',
                    persisRef: {
                        name: 'none'
                    },
                    contentType: {
                        name: 'json',
                        contentType: {
                            name: 'json'
                        }
                    }
                });
                break;
            case 'jdbc':
                $scope.writer.data.metadata = []
            default :
                delete $scope.writer.data.path;

        }
    };
    $scope.contentTypeChange = function () {
        switch ($scope.writer.data.contentType.name) {
            case 'syslog':
                $scope.writer.data.contentType.contentType = angular.copy({name: 'json'});
                $scope.writer.data.contentType.syslogFormat = angular.copy({
                    facility: '0',
                    level: '0',
                    priorityType: "DATA",
                    timestampType: "DATA"
                });
                break;
            default :


        }
    };
    $scope.pathContentTypeChange = function () {
        switch ($scope.writer.data.path.contentType.name) {
            case 'syslog':
                $scope.writer.data.path.contentType.contentType = angular.copy({name: 'json'});
                $scope.writer.data.path.contentType.syslogFormat = angular.copy({
                    facility: '0',
                    level: '0',
                    priorityType: "DATA"
                });
                break;
            default :


        }
    };

    $scope.timestampTypeChange = function () {
        switch ($scope.writer.data.contentType.syslogFormat.timestampType) {
            case 'VAL':
                $scope.writer.data.contentType.syslogFormat.timestamp = new Date();
                break;
            default :
                delete $scope.writer.data.contentType.syslogFormat.timestamp;
        }
    }

    $scope.pathTimestampTypeChange = function () {
        switch ($scope.writer.data.path.contentType.syslogFormat.timestampType) {
            case 'VAL':
                $scope.writer.data.path.contentType.syslogFormat.timestamp = new Date();
                break;
            default :
                delete  $scope.writer.data.path.contentType.syslogFormat.timestamp;
        }
    };

    $scope.replaceToSpec = function (contentType) {
        if ('delkv' === contentType.name) {
            contentType.delimit = contentType.delimit.replace(/\\s/g, ' ').replace(/\\t/g, '\t').replace(/\\n/g, '\n');
            contentType.tab = contentType.tab.replace(/\\s/g, ' ').replace(/\\t/g, '\t').replace(/\\n/g, '\n')
        } else if ('del' === contentType.name) {
            contentType.delimit = contentType.delimit.replace(/\\s/g, ' ').replace(/\\t/g, '\t').replace(/\\n/g, '\n')

        }
        return contentType;
    };
    $scope.replaceFromSpec = function (contentType) {
        if ('delkv' === contentType.name) {
            contentType.delimit = contentType.delimit.replace(/ /g, '\\s').replace(/\t/g, '\\t').replace(/\n/g, '\\n');
            contentType.tab = contentType.tab.replace(/ /g, '\\s').replace(/\t/g, '\\t').replace(/\n/g, '\\n');
        } else if ('del' === contentType.name) {
            contentType.delimit = contentType.delimit.replace(/ /g, '\\s').replace(/\t/g, '\\t').replace(/\n/g, '\\n');

        }
        return contentType;
    };

    $scope.toRule = function () {
        let dataForm = $scope.writer;
        if (dataForm.data.contentType && dataForm.data.contentType.name) {
            if (dataForm.data.contentType.contentType) {
                dataForm.data.contentType.contentType = $scope.replaceToSpec(dataForm.data.contentType.contentType)
            } else {
                dataForm.data.contentType = $scope.replaceToSpec(dataForm.data.contentType)
            }

        } else if (dataForm.data.path && dataForm.data.path.contentType && dataForm.data.path.contentType.name) {
            if (dataForm.data.path.contentType.contentType) {
                dataForm.data.path.contentType.contentType = $scope.replaceToSpec(dataForm.data.path.contentType.contentType)
            } else {
                dataForm.data.path.contentType = $scope.replaceToSpec(dataForm.data.path.contentType)
            }

        }
        return dataForm;
    };
    $scope.fromRule = function (dataForm) {
        if (dataForm.data.contentType && dataForm.data.contentType.name) {
            if (dataForm.data.contentType.contentType) {
                dataForm.data.contentType.contentType = $scope.replaceFromSpec(dataForm.data.contentType.contentType)
            } else {
                dataForm.data.contentType = $scope.replaceFromSpec(dataForm.data.contentType)
            }

        } else if (dataForm.data.path && dataForm.data.path.contentType && dataForm.data.path.contentType.name) {
            if (dataForm.data.path.contentType.contentType) {
                dataForm.data.path.contentType.contentType = $scope.replaceFromSpec(dataForm.data.path.contentType.contentType)
            } else {
                dataForm.data.path.contentType = $scope.replaceFromSpec(dataForm.data.path.contentType)
            }

        }
        return dataForm;
    };

    //保存
    $scope.saveWriter = function (formValid) {

        if (!formValid) return false;
        let dataForm = $scope.toRule();


        Writer.save(dataForm, function (rt) {
            $scope.message = rt;
            if ($scope.message.status === '200') {
                $scope.reload();
                $('.hla-widget-add-table').slideUp();
            }
        });

    };
    $scope.changeDBType = function () {
        let type = $scope.writer.data.protocol;
        let port = null;
        switch (type) {
            case 'mysql':
                port = 3306;
                $scope.writer.data.driver = "com.mysql.jdbc.Driver"
                break;
            case 'oracle':
                port = 1521;
                $scope.writer.data.driver = "oracle.jdbc.driver.OracleDriver"
                break;
            case 'sqlserver':
                port = 1433;
                $scope.writer.data.driver = "com.microsoft.jdbc.sqlserver.SQLServerDriver"
                break;
            case 'DB2':
                $scope.writer.data.driver = "com.ibm.db2.jcc.DB2Driver"
                port = 50000;
                break;
            case 'sybase':
                port = 5000;
                $scope.writer.data.driver = "net.sourceforge.jtds.jdbc.Driver"
                break;
            case 'postgresql':
                $scope.writer.data.driver = "org.postgresql.Driver";
                port = 5432;
                break;
            case 'sqlite':
                $scope.writer.data.driver = "org.sqlite.JDBC";
                port = 0;
                break;
            case 'derby':
                $scope.writer.data.driver = "org.apache.derby.jdbc.EmbeddedDriver";
                port = 1527;
                break;
            case 'phoenix':
                $scope.writer.data.driver = "org.apache.phoenix.jdbc.PhoenixDriver";
                port = 2181;
                break;
        }
        $scope.writer.data.port = port;
    };
    $scope.uploadFile = function () {
        let $edit_table = $('.hla-widget-add-table');
        let $search_table = $('.hla-widget-search-table');
        let upload_table = $('.hla-widget-upload-table');
        let file = $scope.myFile;
        let reader = new FileReader();
        reader.onload = function (e) {
            $scope.writer = $scope.fromRule(angular.fromJson(this.result));
            //  delete   $scope.writer.id;
            if ($scope.writer.writeType == 'forward')
                $scope.collectorChange();
            $edit_table.slideDown();
            $search_table.slideUp();
            upload_table.slideUp();
            $scope.$apply();
        };
        reader.readAsText(file);
    };
    $scope.show = function (clazz, id) {

        delete $scope.message;

        let $edit_table = $('.hla-widget-add-table');
        let $search_table = $('.hla-widget-search-table');
        let upload_table = $('.hla-widget-upload-table');

        Util.resetFormValidateState($scope.ds_form);
        switch (clazz) {
            case 'upload':
                $scope.searchDataSourceForm = {};
                $edit_table.slideUp();
                $search_table.slideUp();
                upload_table.slideDown();
                break;
            case 'search':
                $scope.searchDataSourceForm = {};
                $edit_table.slideUp();
                $search_table.slideToggle();
                upload_table.slideUp();
                break;
            case 'add':
                $edit_table.slideToggle();
                $search_table.slideUp();
                upload_table.slideUp();
                $scope.resetForm();
                break;
            case 'edit':
                Writer.get({
                    id: id
                }, function (ds) {
                    $scope.writer = $scope.fromRule(ds);
                    if ($scope.writer.writeType == 'forward') {
                        $scope.collectorChange();
                    }
                });
                $edit_table.slideDown();
                $search_table.slideUp();
                upload_table.slideUp();

                break;
            case 'copy':
                Writer.get({
                    id: id
                }, function (ds) {
                    $scope.writer = ds;
                    $scope.writer.name = $scope.writer.name + '_copy';
                    delete $scope.writer.id;
                    if ($scope.writer.writeType == 'forward')
                        $scope.collectorChange();
                });
                $edit_table.slideDown();
                $search_table.slideUp();
                upload_table.slideUp();

                break;
            default:
                $edit_table.slideUp();
                $search_table.slideUp();
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
        Writer.delete({
            id: $scope.deleteId
        }, function (rt) {
            $scope.message = rt;
            $scope.reload();
        });
        $confirmModal.modal('hide');
    };
    $confirmModal.find('.op_yes').off().click(function () {
        $scope.confirm_yes();
    });
    $scope.reload = function () {

        //$scope.list = Writer.get($scope.page);
        $scope.list = Writer.get($scope.page, function (pages) {
            if (pages.result.length > 0) {
                pages.result = pages.result.map(function (writer) {
                    return writer;
                });
                $scope.page['count'] = pages['totalCount'];
                $scope.page['limit'] = pages['limit'];
                $scope.page['total'] = pages['totalPage'];
            }
            else {
                if (pages['currentPage'] > 1) {
                    $scope.page['current'] = pages['currentPage'] - 1;
                    $scope.page['count'] = pages['totalCount'];
                    $scope.page['start'] = ($scope.page['current'] - 1) * $scope.page['limit'];
                }
                else {
                    $scope.page['current'] = 1;
                    $scope.page['count'] = 0;
                    $scope.page['start'] = 0;
                }
                pages.result = [];
            }
        });
    };

    $scope.addZkAddress = function () {
        let zookeeperConnect = $scope.writer.data.zookeeperConnect;
        if (!zookeeperConnect) {
            zookeeperConnect = $scope.writer.data.zookeeperConnect = [];
        }
        zookeeperConnect.push(['', 2181]);
    };
    $scope.removeZkAddress = function (index) {
        $scope.writer.data.zookeeperConnect.splice(index, 1);
    };
    $scope.addKafAddress = function () {
        let kafAddresses = $scope.writer.data.metadataBrokerList;
        if (!kafAddresses) {
            kafAddresses = $scope.writer.data.metadataBrokerList = [];
        }
        kafAddresses.push(['', 9092]);
    };
    $scope.removeKafAddress = function (index) {
        $scope.writer.data.metadataBrokerList.splice(index, 1);
    };

    $scope.addHostPort = function () {
        let hostPort = $scope.writer.data.hostPorts;
        if (!hostPort) {
            hostPort = $scope.writer.data.hostPorts = [];
        }
        hostPort.push(['', 9200]);
    };
    $scope.removeHostPort = function (index) {
        $scope.writer.data.hostPorts.splice(index, 1);
    };

    $scope.addWriter = function () {
        let writers = $scope.writer.data.writers;
        if (!writers) {
            writers = $scope.writer.data.writers = [];
        }
        writers.push(['', '']);
    };
    $scope.removeWriter = function (index) {
        $scope.writer.data.writers.splice(index, 1);
    };
    $scope.collectorChange = function () {

        let data = $scope.writer.data;
        if (data.id) {
            let collector = null;
            if ($scope.collectors.$resolved) {
                angular.forEach($scope.collectors, function (obj, key) {
                    if (obj.id == data.id) {
                        collector = obj;
                        return true;
                    }
                });
            }

            if (collector) {
                data.host = collector.host;
                data.port = collector.port;
            }
        }

    };
    $scope.caseWriterChange = function (index) {

        //console.log($scope.writer.data.writers[index]);
        let id = $scope.writer.data.writers[index][1].id;
        //console.log(id);
        if (id) {

            if ($scope.writers.$resolved) {
                angular.forEach($scope.writers, function (obj, key) {
                    if (obj.id === id) {
                        $scope.writer.data.writers[index][1] = obj.data;
                        $scope.writer.data.writers[index][1].id = id;
                        return true;
                    }
                });
            }
        }

    };
    $scope.defaultWriterChange = function () {


        let id = $scope.writer.data.default.id;
        if (id) {

            if ($scope.writers.$resolved) {
                angular.forEach($scope.writers, function (obj, key) {
                    if (obj.id === id) {
                        $scope.writer.data.default = obj.data;
                        $scope.writer.data.default.id = id;
                        return true;
                    }
                });
            }

        }

    };
    // 文件下载
    $scope.downloadFile = function (itemId) {

        //        console.log(sqlItemId);

        Writer.download({
            id: itemId
        }, function (response) {
            let fileName = '存储配置';
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


    /**
     * 构造 字段属性
     */
    $scope.addField = function () {
        $scope.writer.data.metadata.push({
            name: '',
            type: '',
            pk: "N",
            default: '',
            nullable: 'Y',
            length: null,
            decimal: null
        })
    };

    /**
     * 删除字段属性
     * @param index
     */
    $scope.deleteField = function (index) {
        $scope.writer.data.metadata.splice(index, 1);
    };

    //获取表结构信息
    $scope.queryFields = function (formValid) {
        if (!formValid) return false;
        let dataForm = $scope.writer;
        Writer.queryfields(dataForm, function (rt) {
            $scope.message = rt;

        });
    };
});
