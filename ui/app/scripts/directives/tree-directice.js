/**
 * Created by zhhuiyan on 16/9/11.
 */
/**
 * ztree适配angular实现
 * 指令:ztree，ng-model：必须绑定，node-click：绑定节点点击事件，node-data：绑定数据源
 * 用例：<ul id=\'xxx\' class=\'ztree\' ztree ng-model=\'selectNode\' node-click=\'nodeClick()\' node-data=\'treeDatas\'></ul>
 */
app.directive('ztree', function () {
    return {
        require: '?ngModel',
        restrict: 'A',
        link: function ($scope, element, attrs, ngModel) {


            let hasNodeClick = attrs.nodeClick && attrs.nodeClick != null,
                nodeClickMethod = hasNodeClick ? attrs.nodeClick.substring(0, attrs.nodeClick.lastIndexOf('(')) : '';
            let hasSetting = attrs.setting && attrs.setting != null;
            let defaultSetting = {
                view: {
                    dblClickExpand: false
                },
                data: {
                    simpleData: {
                        enable: true,
                        idKey: 'id',
                        pIdKey: 'parentId'
                    }
                },
                edit: {},
                async: {
                    enable: false,
                    contentType: 'application/x-www-form-urlencoded',
                    type: 'post',
                    dataType: 'text',
                    url: '',
                    autoParam: [],
                    otherParam: [],
                    dataFilter: null
                },
                callback: {
                    onClick: function (event, treeId, treeNode, clickFlag) {
                        $scope.$apply(function () {
                            ngModel.$setViewValue(treeNode);
                            let method = $scope.$eval(nodeClickMethod);
                            if (hasNodeClick && method) {
                                let tree = $.fn.zTree.getZTreeObj(treeId);
                                let result = method.call(event.target, treeNode, tree, clickFlag);
                                if (result === false) {
                                    event.stopPropagation();
                                }
                            }
                        });
                    },
                    onCollapse: function (event) {
                        event.stopPropagation();
                    },
                    onExpand: function (event) {
                        event.stopPropagation();
                    }
                }
            };
            if (attrs.nodeData) {
                $scope.$watch(attrs.nodeData, function (newValue, oldValue) {
                    if (newValue && newValue.$promise) {
                        newValue.$promise.then(function (nodes) {
                            initTree(element, angular.copy(nodes));
                        });
                    } else {
                        initTree(element, angular.copy(newValue));
                    }
                });
            }
            function initTree(element, nodes) {
                let setting = angular.copy(defaultSetting);
                let $scopeSetting = $scope[attrs.setting];
                if (hasSetting && $scopeSetting) {
                    angular.extend(setting.view, $scopeSetting.view);
                    angular.extend(setting.data, $scopeSetting.data);
                    angular.extend(setting.async, $scopeSetting.async);
                    angular.extend(setting.edit, $scopeSetting.edit);
                    angular.extend(setting.callback, $scopeSetting.callback);
                }
                let tree = $.fn.zTree.init(element, setting, nodes);
                element.on('click', 'span[treenode_switch]', function (event) {
                    event.stopPropagation();
                    let $this = angular.element(this);
                    if ($this.hasClass('root_close') || $this.hasClass('root_open')
                        || $this.hasClass('center_close') || $this.hasClass('center_open')
                        || $this.hasClass('bottom_close') || $this.hasClass('bottom_open')) {
                        let node = tree.getNodeByTId($this.parent().attr('id'));
                        tree.expandNode(node);
                    }
                });
            }
        }
    };
});

app.directive('treeInput', ['$parse', function () {
    return {
        restrict: 'A',
        scope: {
            ngModel: '=',
            data: '=',
            callback: '=',
        },
        replace: true,
        link: function ($scope, element, attrs) {
            $scope.set = function (node, tree) {
                if (node.isParent) {
                    tree.expandNode(node);
                    return false;
                } else {
                    if (!$scope.node) {
                        $scope.node = {};
                    }
                    $scope.node.view = node.name;
                    if ($scope.ngModel) {
                        angular.extend($scope.ngModel, {name: node.name, id: node.id});
                    }
                    if ($scope.callback) {
                        $scope.callback(node.id, node.name);
                    }
                }
            };
            if (attrs.ngModel) {
                if ($scope.node && $scope.node.view)
                    delete $scope.node.view;
                $scope.$watch('ngModel', function (nval, oval) {
                    if (nval) {
                        let tree = $.fn.zTree.getZTreeObj('ztree_' + $scope.$id);
                        $scope.set(nval, tree, 0, $scope);
                        let node = tree.getNodeByParam('id', nval.id);
                        if (node) {
                            tree.expandNode(tree.getNodeByParam('id', node.parentId));
                            tree.selectNode(node);
                        }
                    }
                });
            }

            //添加表单资产类型清除按钮
            $scope.clear = function () {
                delete $scope.node.view;
            };
            function destroy() {
                let tree = $.fn.zTree.getZTreeObj('ztree_' + $scope.$id);
                if (tree) {
                    tree.destroy();
                }
            }

            $scope.$on('$destroy', destroy);

        },
        template: '<div class=\'input-group\'>\
                       <input type=\'text\' placeholder=\'资产类型\' name=\'treeName\' readonly=\'readonly\' class=\'form-control op_log_type\' \
                          data-toggle=\'dropdown\'  ng-model=\'node.view\' required>\
                        <span title=\'清除\' class=\'input-group-addon op_clear_log_type mouse-on\'  ng-click=\'clear()\'>\
                            <i class=\'fa fa-eraser fa-fw\'></i>\
                        </span>\
                        <div class=\'dropdown-menu\'>\
                            <ul id=\'ztree_{{$id}}\' class=\'ztree search\' ztree ng-model=\'node\' \
                                node-click=\'set()\'\
                                node-data=\'data\'>\
                        </ul>\
                        </div>\
                    </div>'
    };
}]);
