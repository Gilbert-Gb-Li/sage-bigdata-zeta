'use strict';

app.factory('Parser', function ($resource) {
    return $resource('/parser/:id', {}, {
            download: {
                 method: 'GET',
                 responseType: 'blob',
                 transformResponse: function(data){
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
            properties: {
                method: 'POST',
                url: '/previewer',
                isArray:true,
                timeout: 60000
            } ,
            preview: {
                method: 'POST',
                url: '/preview',
                isArray:true,
                timeout: 60000
            },
            previewWithSample: {
                method: 'POST',
                url: '/previewWithSample',
                isArray:true,
                timeout: 60000
            }
         }//end
    );
});

