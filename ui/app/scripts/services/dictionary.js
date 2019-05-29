'use strict';

app.factory('Dictionary', function ($resource) {
    return $resource('/dictionary/:id');
});