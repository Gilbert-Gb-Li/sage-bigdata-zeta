'use strict';

app.controller('DictionaryController', function ($scope, $rootScope, $location, $routeParams, Dictionary, Util) {
    //页面目录
    $rootScope.$path = $location.path.bind($location);
    $scope.reload = function () {
        $scope.list = Dictionary.get($scope.page);
    };

    $scope.dictionary = {name: '', key: '', properties: []};
    //列表功能

    //添加按钮
    $scope.add = function () {
        $('.hla-widget-add-table').slideToggle();
        $('.hla-widget-search-table').slideUp();
        $scope.dictionary = {properties: []};
        $scope.resetForm();
    };
    //添加资产
    $scope.addProperty = function (property) {
        $scope.dictionary.properties.push({
            key: '',
            value: '',
            vtype: 'String'

        })
    };
    $scope.deleteProperty = function (index) {
        delete $scope.dictionary.properties[index];
    };
    $scope.add_dictionary = function (formValid) {
        Dictionary.save($scope.dictionary, function () {
            $scope.reload();
        });
        $('.hla-widget-add-table').slideUp();
    };
    //查询按钮
    $scope.search = function () {
        $('.hla-widget-add-table').slideUp();
        $('.hla-widget-search-table').slideToggle();
        $scope.dictionary = {properties: []};
    };
    //搜索按钮
    $scope.searchDictionary = function () {
        $scope.page.search = $scope.searchDictionaryForm;
        $scope.reload();
    };
    //编辑按钮
    $scope.edit = function (id) {
        $scope.dictionary = Dictionary.get({id: id});
        $('.hla-widget-add-table').slideDown();
        $('.hla-widget-search-table').slideUp();
    };
    //减号按钮
    $scope.removeHostEncoding = function (index) {
        $scope.dictionary.properties.splice(index, 1);
    };
    //删除按钮
    $scope.delete = function (id) {
        $scope.delete_id = id;
        $('#confirmModal').modal('show');
    };
    //删除确定
    $scope.confirm_yes = function () {
        Dictionary.delete({id: $scope.delete_id}, function () {
            $scope.reload();
        });
        $('#confirmModal').modal('hide');
    }
    //点击删除modal确定按钮
    $('#confirmModal .op_yes').off().click(function () {
        $scope.confirm_yes();
    });
    //重置表单
    $scope.resetForm = function (value) {
        $scope.addDicForm = angular.copy(value || {});
        Util.resetFormValidateState($scope.add_dic_form);
    };
    //重置添加表单
    $scope.resetForm();
});
