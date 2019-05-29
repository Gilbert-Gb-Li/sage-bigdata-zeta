'use strict';

app.factory('Collector', function ($resource) {
    return $resource('/collector/:id');
});
app.factory('CollectorOpt', function ($resource) {
    return $resource('/collector/:id/:opt', {opt: '@opt', id: '@id'}, {operate: {method: 'PUT'}});
});
