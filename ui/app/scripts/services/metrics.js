/**
 * Created by zhhuiyan on 16/8/12.
 */
'use strict';

app.factory('Metrics', function ($resource) {
    return $resource('/metrics/:collector/:id', {id: '@id', collector: '@collector'},{
        query: {
            method: 'GET',
            isArray: false
        }
    });
});
