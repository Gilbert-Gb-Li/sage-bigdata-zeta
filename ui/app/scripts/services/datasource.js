'use strict';
app.factory('DataSource', function ($resource) {
    return $resource('/datasource/:id', {}, {
        download: {
            method: 'GET',
            responseType: 'blob',
            transformResponse: function (data) {
//                 console.log(data);
                //MESS WITH THE DATA
                var response = {};
                response.data = data;
                response.headers = {
                    'Content-Disposition': 'attachment'

                };
                return response;
            }
        },
        byType: {
            method: 'GET',
            url: '/datasource/type/:type',
            params: {
                type: '@type'
            },
            isArray: true,
            timeout: 60000,
            transformResponse: function (data) {
                var obj = JSON.parse(data);
                obj.forEach(
                    function (c) {
                        if(c.data.type){
                            c['label']= c.data.name + "-" + c.data.type.name
                        }else{
                            c['label']= c.data.name
                        }
                    }
                );
                return obj;
            }
        },
        byTypeWithExclude: {
            method: 'GET',
            url: '/datasource/type/:type/:exclude',
            params: {
                type: '@type',
                exclude: '@exclude'
            },
            isArray: true,
            timeout: 60000,
            transformResponse: function (data) {
                var obj = JSON.parse(data);
                obj.forEach(
                    function (c) {
                        if(c.data.type){
                            c['label']= c.data.name + "-" + c.data.type.name
                        }else{
                            c['label']= c.data.name
                        }
                    }
                );
                return obj;
            }
        },
        preview: {
            method: 'POST',
            url: '/datasource/preview',
            isArray: true,
            timeout: 60000
        }
    });
});
