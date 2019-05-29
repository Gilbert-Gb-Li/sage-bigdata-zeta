'use strict';
/**
 * a标签二次点击刷新路由
 * 指令:reload-when-click,必须指定 ng-href 或者 href 属性
 * 用例:<a ng-href=\'#/action\' reload-when-click>action</>
 */
app.directive('reloadWhenClick', ['$location', '$route',
    function ($location, $route) {
        return {
            restrict: 'A',
            link: function ($scope, element, attrs) {
                let href = attrs.ngHref || element.attr('href');
                element.click(function () {
                    if (href.substring(2) == $location.path()) {
                        $route.reload();
                    }
                });
            }
        };
    }
]);


app.directive('fileModel', ['$parse', function ($parse) {
    return {
        restrict: 'A',
        link: function (scope, element, attrs) {
            let model = $parse(attrs.fileModel);
            let modelSetter = model.assign;

            element.bind('change', function () {
                scope.$apply(function () {
                    modelSetter(scope, element[0].files[0]);
                });
            });
        }
    };
}]);
app.directive('fileInput', function () {
    return {
        restrict: 'A',
        link: function (scope, element, attrs) {
            $(element).fileinput({
                'showUpload': false,
                'previewFileType': 'text',
                language: 'zh', //设置语言
                allowedFileExtensions: ['json', 'txt'],//接收的文件后缀
                dropZoneEnabled: true,//是否显示拖拽区域

            });
        }
    };
});
app.directive('chart', function () {
    return {
        restrict: 'E',
        scope: {
            width: '@',
            height: '@',
            margin: '@',
            data: '='
        },
        controller: function ($scope, $element, $attrs, TimeSeriesChart) {
            let chart = TimeSeriesChart.getChart($element[0], {
                width: $scope.width,
                height: $scope.height,
                margin: $scope.margin
            });
            chart.render($scope.data);
        }
    };
});
app.directive('sparkLine', function () {
    return {
        restrict: 'E',
        scope: {
            width: '@',
            height: '@',
            data: '='
        },
        controller: function ($scope, $element, $attrs, SparkLineChart) {
            let chart = SparkLineChart.getChart($element[0], {
                width: $scope.width,
                height: $scope.height,
            });
            chart.render($scope.data);
        }
    };
});

app.directive('page', [function () {
    return {
        replace: true,
        template: '<div class=\'hla-widget-foot\'>\
            <ul class=\'pagination pagination-top\'>\
                <li>\
                    <span class=\'bor-none pad-0\'>每页显示 \
                        <select ng-model=\'page.limit\' ng-change=\'load()\' ng-blur=\'first()\' ng-options=\'x for x in [10,20,50]\'></select>\
                    </span>\
                </li>\
                <li ng-if=\'page.current<=1\' class=\'disabled\'><span class=\'op_prev bor-r-5\'><i class=\'glyphicon glyphicon-chevron-left\'></i></span></li>\
                <li ng-if=\'page.current>1\'><span class=\'op_prev bor-r-5\' style=\'cursor:pointer;\' ng-click=\'prev()\'><i class=\'glyphicon glyphicon-chevron-left\'></i></span></li>\
                <li ng-if=\'page.total>0\'><span class=\'op_page_info pad-5 bor-none\'>{{page.current}}/{{page.total}}</span></li>\
                <li ng-if=\'page.total==0\'><span class=\'op_page_info pad-5 bor-none\'>{{page.current}}/1</span></li>\
                <li ng-if=\'page.current>=page.total\' class=\'disabled\'><span class=\'op_next bor-r-5\'><i class=\'glyphicon glyphicon-chevron-right\'></i></span></li>\
                <li ng-if=\'page.current<page.total\'><span class=\'op_next bor-r-5\' style=\'cursor:pointer;\' ng-click=\'next()\'><i class=\'glyphicon glyphicon-chevron-right\'></i></span></li>\
            </ul>\
            <ul class=\'pagination pagination-bottom\'>\
                <li ng-if=\'page.count>0\'>显示第 {{page.start+1}} 到 {{(page.start+page.limit) < page.count? (page.start+page.limit):page.count}} 条数据 , 共 {{page.count}} 项</li>\
                <li ng-if=\'page.count==0\'>暂无数据,共 {{page.count}} 项</li>\
            </ul>\
        </div>',
        link: function (scope, ele, attrs) {

            scope.page = {
                start: 0,
                limit: 10,
                current: 1,
                count: 0,
                total: 1,
                search: {},
                order: 'desc',
                orderBy: 'create_time'
            };
            if (!scope[attrs.method]) {
                throw new Error('load method is undefined');
            }
            scope.first = function () {
                scope.page.start = 0;
                scope.page.current = 1;
                scope.load();
            };
            scope.prev = function () {
                if (scope.page.current > 1) {
                    scope.page.current--;
                    scope.page.start -= scope.page.limit;
                    scope.load();
                }
            };
            scope.next = function () {
                if (scope.page.current < scope.page.total) {
                    scope.page.start += scope.page.limit;
                    scope.page.current++;
                    scope.load();
                }
            };
            scope.check = function () {
                if (scope.page.current >= scope.page.total) {
                    scope.page.current = scope.page.total;
                    scope.page.start = (scope.page.current - 1) * scope.page.limit;
                } else if (scope.page.current <= 1) {
                    scope.page.current = 1;
                    scope.page.start = 0;
                }
            };
            scope.last = function () {
                scope.page.current = scope.page.total;
                scope.page.start = (scope.page.current - 1) * scope.page.limit;
                scope.load();
            };
            //调用
            scope.load = function (to) {


                to && (scope.page.current = to);


                scope[attrs.method]();
                scope.list.$promise.then(function (list) {

                    scope.page.count = list.totalCount;
                    scope.page.total = Math.ceil(list.totalCount / scope.page.limit);
                    if (scope.page.current > 1 && scope.page.current < scope.page.total) {
                        scope.pages = [
                            scope.page.current - 1,
                            scope.page.current,
                            scope.page.current + 1
                        ];
                    } else if (scope.page.current == 1 && scope.page.total > 1) {
                        scope.pages = [
                            scope.page.current,
                            scope.page.current + 1
                        ];
                    } else if (scope.page.current == scope.page.total && scope.page.total > 1) {
                        scope.pages = [
                            scope.page.current - 1,
                            scope.page.current
                        ];
                    }
                });
            };
            scope.load();

            scope.$watch("page.count",function (newVal,oldVal) {
                scope.load();
            })
        }
    }
}]);

app.directive('sage-bigdata-etlTabs', function () {
    return {
        restrict: 'E',
        transclude: true,
        scope: {},
        template: `
            <div class='sage-bigdata-etlTabs'>
                <ul class='nav nav-tabs' role="tablist">
                    <li role="presentation" ng-click='select(pane)' ng-repeat='pane in scopes' ng-class='{active:pane.selected}'>
                        <a>{{pane.tab}}</a>
                    </li>
                </ul>
                <div ng-transclude class='tab-content'></div>
            </div>
        `,
        controller: function ($scope) {
            //使用controller让最内层指令来继承外层指令，
            // 这样内层就可以通过scope的传导，来与外层指令进行数据之间的传递
            var panes = $scope.scopes = [];//

            $scope.select = function (pane) {//实现tabs功能
                angular.forEach(panes, function (scope) { //遍历所有内存指令scope，统一隐藏内容。
                    scope.selected = false;
                });
                pane.selected = true;//通过ng-repeat只
            };

            this.addScope = function (scope) {//由内层指令来继承，把内层指令的scope，传到进外层指令进行控制
                if (panes.length === 0) {
                    $scope.select(scope);
                }
                panes.push(scope);//把内层指令数组，传入外层指令scope数组。
            }
        }
    }
}).directive('sage-bigdata-etlTab', function () {
    return {
        restrict: 'EA',
        scope: {
            tab: '@'
        },
        transclude: true,
        require: '^sage-bigdata-etlTabs',//继承外层指令
        template: `
            <div class="tab-pane" ng-show="selected" ng-transclude></div>
            `,
        link: function (scope, elemenet, attrs, myTabsController) {
            myTabsController.addScope(scope);
            //把内层指令的scope存入到外层指令中，让外层遍历。
        }
    }
});
