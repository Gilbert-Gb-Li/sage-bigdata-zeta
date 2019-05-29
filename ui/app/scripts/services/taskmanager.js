'use strict';

app.factory('TaskManager', function ($resource) {
    return $resource('/taskManager/:id', {}, {
         download: {
             method: 'GET',
             responseType: 'blob',
             transformResponse: function(data){
                 const response = {};
                 response.data = data;
                 response.headers = {
                     'Content-Disposition': 'attachment'

                 };
                 return response;
             }
         } ,
            getTaskLogMsg: {
            method: 'GET',
                url: '/taskManager/taskId/:taskId',
                params: {
                    taskId: '@taskId',
            },
            isArray: true,
                timeout: 5000
        }
    } );
});
