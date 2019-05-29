/**
 * Created by zhhuiyan on 2017/5/8.
 */
const login = angular.module('login', ['ngCookies']);
login.controller('LoginController', function ($http, $scope, $cookies, $rootScope) {
    let vm = this;
    $cookies.remove('TOKEN');
    $cookies.remove('CURRENTUSER');
    $cookies.remove('CURRENTUSERID');
//    function validateLicense() {
//      $http({
//        url:'system/auth/validateLicense',
//        method:'GET'
//      }).then(function (response) {
//        var data=response.data;
//        if(data.statusCode==1001){
//          $cookies.put('license',data.messages[0]);
//          window.location.replace('license.html');
//        }
//      })
//    }
//    validateLicense();
    vm.login = function () {
//      if(!vm.username){
//       var userImg=angular.element('#userImg');
//       userImg.prev().focus();
//       return false
//       }
//       if(!vm.password){
//       var pwdImg=angular.element('#pwdImg');
//       pwdImg.prev().focus();
//       return false
//       }
//       if(!vm.captcha){
//       var captcha=angular.element('#captcha');
//       captcha.focus();
//       return false
//       }
//      if (!valid) return false;

        $http({
            url: '/sign-in',
            method: 'POST',
            data: {
                realm: 'zeta',
                creds:[{accountName: vm.username,password: vm.password}]//,
              //  ucode: vm.username, password: vm.password
//          captcha: vm.captcha,
//          usecaptcha: vm.captcha==vm.password?'yes':'no'
            }
        }).then(function (response) {
            let data = response.data;
            if (data.success) {
                $cookies.put('TOKEN', data.account.name);
                $cookies.put('CURRENTUSER', data.account.props.email);
                window.location.replace('index.html');
            }
//             if (data.result) {
//                 $cookies.put('TOKEN', data.data.token);
//                 $cookies.put('userId', data.data.userId);
//                 $cookies.put('CURRENTUSER', data.data.user.name);
//
// //
// //        }else if(data.statusCode==1001){
// //          window.location.replace('license.html');
//             }

            else {

                vm.error = '用户名密码错误'
                }
                // if (data.code && data.code == 1) {
                //     vm.error = data.msg+",请联系管理员!";
                // } else {
                //     vm.error = '用户名或密码错误!'
                // }

//          var $captchaImg = angular.element('#captcha_img');
//          refreshCaptcha($captchaImg);
            //}
        }, function (response) {
            vm.error = '服务器错误,请联系管理员!';

            //window.location.replace('/');
            console.log(response);
        });
    };
//    vm.refreshCaptcha = function ($event) {
//      var $captcha = angular.element($event.target);
//      refreshCaptcha($captcha);
//    };
//    //刷新验证码
//    function refreshCaptcha($captcha) {
//      var newSrc = $captcha.attr('src');
//      if (newSrc.includes('v=')) {
//        newSrc = newSrc.substring(0, newSrc.lastIndexOf('?'));
//      }
//      newSrc += '?v=' + new Date().getTime();
//      $captcha.attr('src', newSrc);
//    }
});
