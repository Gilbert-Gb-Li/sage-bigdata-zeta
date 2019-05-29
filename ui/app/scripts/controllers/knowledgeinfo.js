//Created by liyju on 2017/11/22.

'use strict';
app.controller('KnowledgeInfoController', function ($scope, $rootScope, $location, Util,Knowledge,KnowledgeInfo,Modeling) {
    $rootScope.$path = $location.path.bind($location);
    $scope.encodings = ['UTF-8', 'GBK', 'GB18030', 'GB2312', 'BIG5', 'UNICODE', 'ASCII', 'ISO-8859-1'];
    const $confirmModal = $('#confirmModal');

    $scope.knowledges = [];

    //知识库列表
     Knowledge.queryByStatus(function (knowledges){
        knowledges.map(function (knowledge) {
            $scope.knowledges.push(knowledge)
        })
    });
    //离线模型列表
    $scope.modelings = Modeling.queryByType({type: 'model'},function (modelings ){
        modelings.map(function (modeling) {
            $scope.knowledges.push(modeling)
        })
    });
    $scope.tableTheads= [];
    $scope.reload = function () {
        $scope.tableTheads= [];
        $scope.list =  KnowledgeInfo.get($scope.page, function (pages) {
            if( pages.result.length > 0 ) {
                pages.result = pages.result.map(function (knowledgeinfo) {
                    //console.log(knowledgeinfo)
                   return knowledgeinfo;
                });
                for(var key in pages.result[0]){
                    $scope.tableTheads.push(key)
                }
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
    let $search_table = $('.hla-widget-search-table');
    $search_table.slideToggle();
});