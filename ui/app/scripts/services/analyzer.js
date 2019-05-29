app.factory('Analyzer', function ($resource) {
    return $resource('/analyzer/:id', {}, {
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
            preview: {
                method: 'POST',
                url: '/analyzer/preview',
                timeout: 60000
            },
            queryByType: {
                method: 'GET',
                url: '/analyzer/type',
                isArray: true,
                timeout: 60000
            }
        }//end
    )
});