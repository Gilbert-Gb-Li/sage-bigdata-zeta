'use strict';

app.controller('CollectorController', function($scope, $rootScope, $location, $routeParams, Collector, CollectorOpt) {
  const $confirmModal = $('#collectModal');
  //页面目录
  $rootScope.$path = $location.path.bind($location);
  $scope.reload = function() {
     //$scope.list = Collector.get($scope.page);

      $scope.list = Collector.get($scope.page, function (pages) {
          if (pages.result.length > 0) {
              pages.result = pages.result.map(function (collector) {
                  return collector;
              });
              $scope.page['count'] = pages['totalCount'];
              $scope.page['limit'] = pages['limit'];
              $scope.page['total'] = pages['totalPage'];
          } else {
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
  //删除按钮
  $scope.delete = function(id, status) {
    $scope.id = id;
      if(status==='UNAVAILABLE'){
        $('#collectModal .modal-body p').html('采集器不存在或者已经停止,删除有可能造成数据不一致,确认删除?')
      }else{
          $('#collectModal .modal-body p').html('确认要删除吗?')
      }
    $confirmModal.modal('show');
  };
  $scope.operate = function(opt, id) {
    CollectorOpt.operate({
      id: id,
      opt: opt
    }, function(rt) {
      $scope.message = rt;
      $scope.reload();
    });
  };
  //删除确定
  $scope.confirm_yes = function() {
    Collector.delete({
      id: $scope.id
    }, function(rt) {
      $scope.message = rt;
      $scope.reload();
    });
    $confirmModal.modal('hide');
  };
  $confirmModal.find('.op_yes').off().click(function() {
    $scope.confirm_yes();
  });

});
