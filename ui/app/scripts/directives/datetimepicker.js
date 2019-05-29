/**
 * Description: datetimepicker
 * Author: xinfeng_qian
 * Update: xinfeng_qian(2016-09-19 18:22)
 */
app.directive('datetimePicker', function () {

		return {
			require: '?ngModel',
			restrict: 'AE',
            scope: {
			    datetimePicker: '='
            },
			link: function (scope, elem, attr, ngModel) {
				let defOptions={
					format: 'YYYY-MM-DD HH:mm:ss'
				};
				let options=angular.merge({},defOptions,scope.datetimePicker||{});
				//input-group里的按钮点击
				let parent=elem.parent();
				if (parent.hasClass('input-group')) {
					let btn=null;
					// in case there is more then one 'input-group-addon' Issue #48
					if (parent.find('.datepickerbutton').length === 0) {
						btn = parent.find('.input-group-btn');
					} else {
						btn = parent.find('.datepickerbutton');
					}
					btn.click(function () {
                        elem.datetimepicker('show');
					});
				}
				elem.datetimepicker(options).on('dp.change', function () {
					ngModel.$setViewValue(this.value);
				});
				scope.$watch('datetimePicker', function (n, o) {
				    if (n === o) return;
                    let dp = getInstance(elem);
                    dp.options(scope.datetimePicker);
                    if (scope.datetimePicker.defaultDate) {
                        dp.date(scope.datetimePicker.defaultDate);
                        dp.viewDate(scope.datetimePicker.defaultDate); // default viewDate is moment(), bug #1325
                    }
                }, true);
				//渲染
                ngModel.$render = function() {
                    let date=ngModel.$viewValue?moment(ngModel.$viewValue):null;
                    getInstance(elem).date(date);
                };
				//修复viewMode为years或months时显示days日期选择界面的bug
				elem.on('dp.show', function (event) {
					let instance = getInstance(angular.element(event.target));
					let viewMode=instance.options().viewMode;
                    if (viewMode == 'years' || viewMode == 'months') {
						instance.viewMode(viewMode);
					}
				});
				//获取实例
				function getInstance(element) {
                    return element.data('DateTimePicker');
				}
			}

		};
	});
