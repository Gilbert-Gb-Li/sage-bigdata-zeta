app.factory('Knowledge', function ($resource) {
    return $resource('/knowledge/:id', {}, {
            download: {
                method: 'GET',
                responseType: 'blob',
                transformResponse: function (data) {
//                 console.log(data);
                    //MESS WITH THE DATA
                    let response = {};
                    response.data = data;
                    response.headers = {
                        'Content-Disposition': 'attachment'

                    };
                    return response;
                }
            },
            operate: {
                method: 'PUT',
                url: '/knowledge/:opt/:id',
                params: {
                    opt: '@opt',
                    id: '@id'
                }

            } ,
            queryByStatus:{
                method: 'POST',
                url:'/knowledge/queryByStatus',
                isArray: true
            }
        }//end
    )
});