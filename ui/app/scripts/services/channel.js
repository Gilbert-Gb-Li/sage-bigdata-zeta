'use strict';
app.factory('Channel', function ($resource) {
    return $resource('/config/:id', {}, {
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
            url: '/config/:opt/:id',
            params: {
                opt: '@opt',
                id: '@id'
            },
            timeout: 20000
        },
        check: {
            method: 'POST',
            url: '/usability',
            isArray: true,
            timeout: 20000
        }

    });
});
app.service('Util', function () {
    /**
     * 重置表单状态
     * @param form
     * @returns {boolean}
     */
    this.resetFormValidateState = function (form) {
        if (!form) return false;
        form.$submitted = false;
        angular.forEach(form, function (obj, key) {
            if (key.substring(0, 1) !== '$' && form[key].$dirty) {
                form[key].$dirty = false;
                form[key].$pristine = true;
            }
        });
    };
});
