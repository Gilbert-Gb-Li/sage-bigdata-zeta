'use strict';
const auth = true;
const app = angular.module('raysdata.sage-bigdata-etl',
    ['ngAnimate', 'uiSwitch', 'ui.checkbox', 'ui.ace', 'ngRoute', 'ngResource', 'ui.bootstrap', 'nvd3', 'ngCookies','smart-table']);

app.config(function ($locationProvider, $routeProvider, $httpProvider) {
    $httpProvider.defaults.headers.put['Content-Type'] = 'application/json;charset=utf-8';
    $httpProvider.defaults.headers.put['Content-Encoding'] = 'UTF-8';
    $httpProvider.defaults.headers.post['Content-Type'] = 'application/json;charset=utf-8';
    $httpProvider.defaults.headers.post['Content-Encoding'] = 'UTF-8';
    $locationProvider.html5Mode(false);

    $routeProvider
        .when('/', {
            templateUrl: 'views/channel.html',
            controller: 'ChannelController'
        }).when('/knowledge', {
        templateUrl: 'views/knowledge.html',
        controller: 'KnowledgeController'
    }).when('/knowledgeinfo', {
        templateUrl: 'views/knowledgeinfo.html',
        controller: 'KnowledgeInfoController'
    }).when('/datasource', {
        templateUrl: 'views/datasource.html',
        controller: 'DataSourceController'
    }).when('/dictionary', {
        templateUrl: 'views/dictionary.html',
        controller: 'DictionaryController'
    }).when('/collector', {
        templateUrl: 'views/collector.html',
        controller: 'CollectorController'
    }).when('/parser', {
        templateUrl: 'views/parser/list.html',
        controller: 'ParserController'
    }).when('/analyzer', {
        templateUrl: 'views/analyzer/list.html',
        controller: 'AnalyzerController'
    }).when('/taskManager', {
        templateUrl: 'views/taskmanager.html',
        controller: 'TaskManagerController'
    }).when('/writer', {
        templateUrl: 'views/writer.html',
        controller: 'WriterController'
    }).when('/metrics', {
        templateUrl: 'views/metrics.html',
        controller: 'MetricsController'
    }).when('/modeling', {
        templateUrl: 'views/modeling.html',
        controller: 'ModelingController'
    }).otherwise({
        redirectTo: '/'
    });
    let paceOptions = {
        elements: false,
        //大于20ms 的请求显示进度
        restartOnRequestAfter: 20
    };
    angular.extend(Pace.options, paceOptions);

    //初始化httpProvider
    function httpInit(provider) {
        httpInterceptor.$inject = ['$q', '$rootScope', 'AUTH_EVENTS', 'TOKEN'];

        function httpInterceptor($q, $rootScope, AUTH_EVENTS, TOKEN,CURRENTUSERID) {
            console.log('into interceptor......');
            return {
                request(config) {
                    let token = TOKEN.get();
                    //let currentUserId = CURRENTUSERID.get();
                    if (auth) {
                        if (!token || token.trim() === '') {
                            window.location.replace('login.html')
                        }
                    }
                    config.headers[TOKEN.key] = token;
                    //config.headers[CURRENTUSERID.key] = currentUserId;
                    return config;
                },
                response(response) {
                    let data = response.data;
                    if (angular.isObject(data)) {
                        /*//会话超时
                        if (data.statusCode !== undefined && data.statusCode === 3) {
                            $rootScope.$broadcast(AUTH_EVENTS.SessionTimeout);
                            throw AUTH_EVENTS.SessionTimeout;
                        }
                        //License过期
                        if (data.statusCode !== undefined && data.statusCode === 1001) {
                            $rootScope.$broadcast(AUTH_EVENTS.LicenseExpired,data);
                            throw AUTH_EVENTS.LicenseExpired;
                        }*/
                    }
                    return response;
                },
                responseError(rejection) {
                    return $q.reject(rejection);
                }
            }
        }

        provider.interceptors.push(httpInterceptor);
    }

    httpInit($httpProvider);
});

app.controller('HeaderController', function ($scope, CURRENTUSER) {
    //$scope.currentUser = TOKEN.get('superadmin')
    console.log(CURRENTUSER);
    $scope.currentUser = CURRENTUSER.get('CURRENTUSER');
});

