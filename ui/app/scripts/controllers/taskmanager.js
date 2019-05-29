'use strict';

app.controller('TaskManagerController', function ($scope, $rootScope, $location, Util, TaskManager, Channel, Modeling, Knowledge, Collector, DataSource) {
    $rootScope.$path = $location.path.bind($location);
    $scope.datasources = DataSource.query();
    $scope.channels = DataSource.byType({type: "channel"});
    $scope.modelings = Modeling.query();
    $scope.knowledges = Knowledge.query();

    const $confirmModal = $('#confirmModal');
    const $catDetailModal = $('.hla-widget-cat-log-table');

    const TaskDefault = {

        data: {
            enable: false,
            jobType: 'channel',
            action: 'START',
            retry:3,
            retryInterval:10,
        }
    };
    //采集器
    $scope.collectors = Collector.query();
    //重置表单
    $scope.resetForm = function () {
        $scope.task = angular.copy(TaskDefault);
        Util.resetFormValidateState($scope.ds_form);
    };
    $scope.resetForm();

    //保存
    $scope.saveTimer = function (formValid) {
        if (!formValid) return false;

        let dataForm = $scope.task;
        let config = $scope.getTaskName($scope.task.data.jobType,$scope.task.data.configId)[0];
        dataForm.data.taskName= config['name'];
        TaskManager.save(dataForm, function (rt) {
            $scope.message = rt;
            if ($scope.message.status === '200') {
                $scope.reload();
                $('.hla-widget-add-table').slideUp();
            }
        });
    };

    $scope.getTaskName = function (jobType,configId) {
        if("channel"===jobType)
            return $scope.channels.filter(function (channel) {
                return channel.id === configId;

            });
        if("modeling"===jobType)
            return $scope.modelings.filter(function (modeling) {
                return modeling.id === configId;

            });
        if("knowledge"===jobType)
            return $scope.knowledges.filter(function (knowledge) {
                return knowledge.id === configId;

            });

    };
    $scope.uploadFile = function () {
        delete $scope.message;
        let $edit_table = $('.hla-widget-add-table');
        let $search_table = $('.hla-widget-search-table');
        let upload_table = $('.hla-widget-upload-table');
        let file = $scope.myFile;
        let reader = new FileReader();
        $scope.all = TaskManager.query();
        let allList = [];
        if ($scope.all.length > 0) {
            $scope.all.forEach(function (value) {
                allList.push(value.id);
            });
        }
        reader.onload = function () {
            $scope.task = angular.fromJson(this.result);
            //删除定时器的id
            delete $scope.task.id;
            //判断数据通道，离线建模。知识库是否存在不存在，删除configId
            if ($scope.task.data.jobType==='channel'){
                let configList = $scope.channels.filter(function (channel) {
                    return channel.id === $scope.task.data.configId;

                });
                if (configList.length === 0) {
                    delete $scope.task.data.configId;
                }
            }else if($scope.task.data.jobType==='modeling'){
                let modelingList = $scope.modelings.filter(function (modeling) {
                    return modeling.id === $scope.task.data.configId;

                });
                if ( modelingList.length === 0) {
                    delete  $scope.task.data.configId;
                }
            }else if($scope.task.data.jobType==='knowledge'){
                let knowledgeList = $scope.knowledges.filter(function (knowledge) {
                    return knowledge.id ===  $scope.task.data.configId;

                });
                if (knowledgeList.length === 0) {
                    delete $scope.task.data.configId;
                }
            }else
                delete $scope.task.data.configId;

            $edit_table.slideDown();
            $search_table.slideUp();
            upload_table.slideUp();
            $scope.$apply();
        };
        reader.readAsText(file);
    };
    $scope.show = function (clazz, id) {
        delete  $scope.message;
        let $edit_table = $('.hla-widget-add-table');
        let $search_table = $('.hla-widget-search-table');
        let upload_table = $('.hla-widget-upload-table');
        let $cat_log_table = $('.hla-widget-cat-log-table');
        Util.resetFormValidateState($scope.ds_form);
        switch (clazz) {
            case 'upload':
                $scope.searchDataSourceForm = {};
                $edit_table.slideUp();
                $search_table.slideUp();
                upload_table.slideDown();
                $cat_log_table.slideUp();
                break;
            case 'search':
                $scope.searchDataSourceForm = {};
                $edit_table.slideUp();
                $search_table.slideToggle();
                upload_table.slideUp();
                $cat_log_table.slideUp();
                break;
            case 'add':
                $edit_table.slideToggle();
                $search_table.slideUp();
                upload_table.slideUp();
                $cat_log_table.slideUp();
                $scope.resetForm();
                break;
            case 'edit':
                TaskManager.get({id: id}, function (ds) {
                    $scope.task = ds;
                });
                $edit_table.slideDown();
                $search_table.slideUp();
                upload_table.slideUp();
                $cat_log_table.slideUp();
                break;
            case 'copy':
                TaskManager.get({id: id}, function (ds) {
                    $scope.task = ds;
                    $scope.task.name = $scope.task.name + '_copy';
                    delete   $scope.task.id;
                    $scope.task.data.msg = []
                });
                $edit_table.slideDown();
                $search_table.slideUp();
                upload_table.slideUp();
                $cat_log_table.slideUp();

                break;
            case 'catLog':
                $scope.taskLogInfo =  TaskManager.getTaskLogMsg({taskId: id});
                $edit_table.slideUp();
                $search_table.slideUp();
                upload_table.slideUp();
                $cat_log_table.slideDown();

                break;
            default:
                $edit_table.slideUp();
                $search_table.slideUp();
                upload_table.slideUp();
                $cat_log_table.slideUp();
                break;
        }

    };

    //删除按钮
    $scope.delete = function (id) {
        $scope.deleteId = id;
        $confirmModal.modal('show');
    };


    //查看详情
    $scope.catDetail = function (task) {
        $scope.task = task;
        $catDetailModal.modal('show');
    };

    //删除确定
    $scope.confirm_yes = function () {
        let $edit_table = $('.hla-widget-add-table');
        TaskManager.delete({id: $scope.deleteId}, function (rt) {
            $scope.message = rt;
            $edit_table.slideUp();
            $scope.reload();
        });
        $confirmModal.modal('hide');
    };


    $confirmModal.find('.op_yes').off().click(function () {
        $scope.confirm_yes();
    });



    $scope.reload = function () {
        let $cat_log_table = $('.hla-widget-cat-log-table');
        $cat_log_table.slideUp();
        $scope.list = TaskManager.get($scope.page, function (pages) {
            if (pages.result.length > 0) {
                pages.result = pages.result.map(function (task) {
                    return task;
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

    //文件下载
    $scope.downloadFile = function (itemId) {


        TaskManager.download({ id: itemId }, function (response) {
            let fileName = '定时器配置';
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
});
