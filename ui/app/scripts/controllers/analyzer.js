'use strict';


app.controller('AnalyzerController', function ($scope, $rootScope, $location, Modeling, DataSource, Analyzer,Parser, Util) {

    $scope.aceLoaded = function (_editor) {
        // Options
        //_editor.setReadOnly(true);
    };
    $scope.aceValue = 'Foobar';
    $scope.aceChanged = function (e) {
        //
    };

    $scope.previewData = {};
    //数据通道
    const allChannel = DataSource.byType({type: "channel"});
    $scope.channels = [];
    //分析规则
    $scope.analyzers = Analyzer.query();
    $scope.parsers = Parser.query();
    //数据模型
    $scope.modelings = Modeling.queryByType({type: 'model'});
    $scope.showSelect = function () {
        if($scope.rule.data.name == 'sql'){
            allChannel.$promise.then(ac=>{
                $scope.channels = ac.filter(c=>c.data.name == 'single-table' || (c.data.name == 'tuple' && c.data.on.name=='sql'));
            });
        }else {
            allChannel.$promise.then(ac=>{
                $scope.channels = ac.filter(c=> c.data.name!='single-table' ).filter(c=> c.data.name != 'tuple' || c.data.on.name=='join');
                console.log($scope.channels);
            });
            //获取参考的数据通道
            $scope.channel = $scope.channels.filter(c => c.id === $scope.rule.channel).shift();
            if(!$scope.channel) {
                if($scope.rule.channel){
                    delete $scope.rule.channel;
                }
            }else  {
                $scope.timefields = $scope.channel.data.parser.metadata.filter(m => m[1] === 'datetime' || m[1] === 'long');
            }
        }
    };
    /**
     * 构造 字段属性
     */
    $scope.changeAnaType = function () {
        switch ($scope.rule.data.name) {
            case "svm":
                break;
            case "regression":
                $scope.rule.data.degree = 1;
                break;

        }
        $scope.showSelect();

    };

    $scope.addProperty = function () {
        if ($scope.rule.channel) {
            if (!$scope.rule.data.fromFields) {
                $scope.rule.data.fromFields = [];
            }
            $scope.rule.data.fromFields.push(['', 'string']);

        } else {
            $scope.confirm = {
                title: '提示',
                message: "您未选择解析规则，确认添加字段么？",
                ok: function () {
                    $scope.rule.data.fromFields.push(['', 'string']);
                    $('#commonConfirmModal').modal('hide');
                },
                cancel: function () {
                    $('#commonConfirmModal').modal('hide');
                }
            };
            $('#commonConfirmModal').modal('show');
        }

    };
    /**
     * 删除字段属性
     * @param index
     */
    $scope.deleteProperty = function (index) {
        $scope.rule.data.fromFields.splice(index, 1);
    };

    $scope.addHashField = function () {
        if (!$scope.rule.data.cols) {
            $scope.rule.data.cols = [];
        }
        $scope.rule.data.cols.push(['', {name: "atan"}]);
    };

    $scope.deleteHashField = function (index) {
        $scope.rule.data.cols.splice(index, 1);
    };

    $scope.channelChange = function () {
        $scope.resolver = $scope.channels.filter(function (channel) {
            return channel.id === $scope.rule.channel
        }).shift();
        if ($scope.resolver && $scope.resolver.properties) {
            $scope.timefields = $scope.resolver.properties.filter(function (r) {
                return r.type === 'datetime';
            })
        }
    };

    $scope.changeFieldName = function (index) {
        let fieldName = $scope.rule.data.fromFields[index][0];
        if ($scope.resolver && $scope.resolver.properties) {
            let p = $scope.resolver.properties.filter(function (p) {
                return  p.key === fieldName}).shift();
            if (p) {
                $scope.rule.data.fromFields[index][1] = p.type;
            }
        }
    };


    $scope.addFilter = function () {
        if (!$scope.rule.data.filter) {
            $scope.rule.data.filter = []
        }
        $scope.rule.data.filter.push({
            name: 'analyzerRedirect',
            cases: []
        });
        $scope.addCase($scope.rule.data.filter.length - 1);
    };
    $scope.removeFilter = function (index) {
        $scope.rule.data.filter.splice(index, 1);
    };
    $scope.addCase = function (index) {
        if (!$scope.rule.data.filter[index].cases) {
            $scope.rule.data.filter[index].cases = []
        }
        $scope.rule.data.filter[index].cases.push({
            name: 'analyzerCase',
            rule: {
                name: 'reAnalyzer'
            }
        })
    };

    $scope.removeCase = function (findex, index) {
        $scope.rule.data.filter[findex].cases.splice(index, 1);
    };
    $scope.caseChange = function (fIndex, index) {
        delete $scope.rule.data.filter[fIndex].cases[index].rule.ref
    };

    $scope.validateForm = function (formValid) {
        if (!formValid) return false;
        if (!formValid.$valid) {
            return false;
        }
        if (!formValid.$submitted) {
            formValid.$submitted = true;
        }
        /*if ($scope.rule.data.name === 'sql') {
            $scope.resolver = $scope.channels.filter(p => p.id === $scope.rule.channel).shift();
            if (!$scope.resolver) {
                $scope.message = {
                    status: '500',
                    title: '错误！',
                    message: '数据一致性错误，请重新选择解析规则'
                };
                return false;
            }
            if ($scope.rule.data.fromFields.length == 0) {
                return false;
            }
            if (!$scope.validateSql()) {
                return false;
            }
        }*/
        return true;
    };

    $scope.saveRule = function (formValid) {
        if (!$scope.validateForm(formValid)) {
            return;
        }
        /*if ($scope.rule.data.timeCharacteristic == 'ProcessingTime' && $scope.rule.data.name == 'sql') {
            $scope.rule.data.timeField = null;
            $scope.rule.data.maxOutOfOrderness = null;
        }*/
        Analyzer.save($scope.rule,
            function (rt) {
                $scope.message = rt;
                if ($scope.message.status === '200') {
                    $scope.reload();
                    $scope.show();
                    $('.hla-widget-add-table').slideUp();
                    $scope.reload();
                }
            });
    };
    $scope.showPreview = false;
    //预览函数
    $scope.preview = function (formValid) {
        //校验字段开始
        delete $scope.message;
        /*if ($scope.rule.data.name == 'sql') {
            let keys = $scope.resolver.properties.map(k => k.key);//解析规则中的key
            let fields = [];
            $scope.rule.data.fromFields.forEach(f => {
                if (!keys.includes(f[0])) {
                    fields.push(f[0]);
                }
            });
            if (fields.length > 0) {
                $scope.message = {
                    status: '400',
                    title: "警告！",
                    message: '解析规则[' + $scope.resolver.name + ']中不存在字段[' + fields.join(",") + '],请确认配置是否正确'
                };
            }
            if ($scope.timefields && $scope.rule.data.timeField) {
                let timefield = $scope.timefields.filter(t => t.key === $scope.rule.data.timeField).shift();
                if (!timefield) {
                    let message = '解析规则[' + $scope.resolver.name + ']中不存在您填写的时间字段[' + $scope.rule.data.timeField + ']，请确认字段是否为时间类型'
                    if ($scope.message) {
                        message = '（1）' + $scope.message.message + '；（2）' + message;
                    }
                    $scope.message = {
                        status: '400',
                        title: "警告！",
                        message: message
                    };

                }
            }
            //校验字段结束

        }*/

        if (!$scope.validateForm(formValid)) {
            return;
        }
        Analyzer.preview($scope.rule, function (data) {

            if (data.status && data.status == 500) {
                $scope.message = {
                    status: '500',
                    title: '错误！',
                    message: data.message,
                };
            } else {
                // delete $scope.message;
                $scope.previewData = data;
                $scope.showPreview = true;
            }
        });
    };
    $scope.reload = function () {
        /*    $scope.list = {};
        $scope.list = Analyzer.get($scope.page);*/

        $scope.list = Analyzer.get($scope.page, function (pages) {
            if (pages.result.length > 0) {
                pages.result = pages.result.map(function (analyer) {
                    return analyer;
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
    //
    $scope.show = function (clazz, id) {
        let edit = $('.hla-widget-add-table');
        let search = $('.hla-widget-search-table');
        let list = $('.hla-widget-data-table');
        let upload_table = $('.hla-widget-upload-table');
        switch (clazz) {
            case 'upload':
                delete $scope.message;
                $scope.searchDataSourceForm = {};
                edit.slideUp();
                search.slideUp();
                upload_table.slideDown();
                break;

            case 'add':
                delete $scope.message;
                $scope.rule = {
                    name: '',
                    data: {
                        name: 'sql',
                        window:{
                            name:"sliding"
                        },
                        timeSeriesModel: {
                            modelType: 'ADDITIVE'
                        },
                     fromFields: []
                    }
                };
                $scope.showSelect();
                //TODO get form data
                Util.resetFormValidateState($scope.add_form);
                edit.slideToggle();
                search.slideUp();
                list.slideToggle();
                upload_table.slideUp();
                break;
            case 'edit':
                delete $scope.message;
                Analyzer.get({id: id}, function (data) {
                    $scope.rule = data;
                    $scope.rule = angular.fromJson($scope.rule)
                    if($scope.rule.data.name == "vectorization" && $scope.rule.data.vectorization.name == "simhash"){
                        $scope.rule.data.vectorization.length = $scope.rule.data.vectorization.length.toString()
                    }
                    $scope.showSelect();
                });
                Util.resetFormValidateState($scope.add_form);
                edit.slideDown();
                search.slideUp();
                upload_table.slideUp();
                list.slideUp();
                break;
            case 'copy':
                delete $scope.message;
                Analyzer.get({id: id}, function (data) {
                    $scope.rule = data;
                    $scope.showSelect();
                    delete $scope.rule.id;
                    $scope.rule.name = $scope.rule.name + '_copy';
//                    $scope.fromRule();
                });
                Util.resetFormValidateState($scope.add_form);
                edit.slideDown();
                search.slideUp();
                upload_table.slideUp();
                list.slideUp();
                break;
            case 'search':
                delete $scope.message;
                edit.slideUp();
                search.slideToggle();
                upload_table.slideUp();
                list.slideDown();
                $scope.page.search = {};
                $scope.page.search.name = null;
                break;
            default:
                edit.slideUp();
                search.slideUp();
                upload_table.slideUp();
                list.slideDown();
        }
    };//

    $scope.downloadFile = function (sqlItemId) {

//        console.log(sqlItemId);

        Analyzer.download({id: sqlItemId}, function (response) {
            let fileName = 'Sql分析规则';
            fileName = decodeURI(fileName);
            let url = URL.createObjectURL(new Blob([response.data]));
            let a = document.createElement('a');
            document.body.appendChild(a); //此处增加了将创建的添加到body当中
            a.href = url;
            a.download = fileName + '-' + sqlItemId + '.json';
            a.target = '_blank';
            a.click();
            a.remove(); //将a标签移除
        }, function (response) {
//            console.log(response);
        });
    };

    $scope.uploadFile = function () {
        delete $scope.message;
        let $edit_table = $('.hla-widget-add-table');
        let $search_table = $('.hla-widget-search-table');
        let upload_table = $('.hla-widget-upload-table');
        let list = $('.hla-widget-data-table');
        let file = $scope.myFile;
        let reader = new FileReader();
        reader.onloadend = function (e) {
//            console.log(this.result);
            $scope.rule = angular.fromJson(this.result);

            try {
                //  delete    $scope.rule.id;
                let flag = false;

//                $scope.fromRule();
                $edit_table.slideDown();
                $search_table.slideUp();
                upload_table.slideUp();
                list.slideUp();
                delete $scope.myFile;
//                console.log(fileInput);
                $scope.$apply();
            } catch (e) {
//                console.log(e);
                $scope.message = {
                    status: '500',
                    title: '错误！',
                    message: '数据解析出错,你检查你的数据文件是否是正确的解析规则内容!'
                };
            }
        };

        try {
            reader.readAsText(file);
        } catch (e) {
            $scope.message = {
                status: '500',
                title: '错误！',
                message: '数据解析出错,你检查你的数据文件是否正确!'
            };

        }


    };//


    $scope.validateSql = function () {
        let sql = $scope.rule.data.sql;
        if (sql) {
            sql = sql.toUpperCase();
            if (sql.indexOf("SELECT") === -1 || sql.indexOf("FROM") === -1) {
                $scope.message = {
                    status: '500',
                    title: '错误！',
                    message: "SQL语句不合法，请填写正确的SQL"
                };
                return false;
            }
            return true;
        }
    };

    $scope.delete = function (id) {
        $scope.delete_id = id;
        $('#confirmModal').modal('show');
    };
    //删除确定
    $scope.confirm_yes = function () {

        Analyzer.delete({id: $scope.delete_id}, function (data) {
            $scope.message = data;
            $('#confirmModal').modal('hide');
            $scope.reload();
        });


    };
    $('#confirmModal').find('.op_yes').off().click(function () {
        $scope.confirm_yes();
    });

    $scope.typeChange = function () {
        //分析规则
        Modeling.queryByType({type: $scope.rule.type}, function (rt) {
            $scope.modelings = rt;
        })
    }
});