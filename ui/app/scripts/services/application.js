/**
 * Description: 授权服务
 * Author: xinfeng_qian
 * Update: xinfeng_qian(2016-10-09 15:16)
 */
app.factory('Auth', function ($resource) {
	//return $resource('json/authorization.json');
    return $resource('system/auth/userInfo');
}).service('AuthService', function (Auth, Session, Util) {
	let _this=this;
	//已登录
	this.hasLogin = function () {
		return !!Session.user;
	};
	//已授权
	this.hasAuthorized = function (authCode) {
		return (_this.hasLogin() && !(Session.auth[authCode]===false));
	};
	//获取授权信息
	this.getAuth=function (sFn,eFn) {
		Auth.get(function (response) {
			if(!Util.response.ok(response)){
				eFn(response);
				return false;
			}
			let data=Util.response.getData(response);
			Session.create(data.user.userId,data.user.loginName,data.auth,data.user);
			sFn(data.user);
		},eFn);
	};
}).constant('AUTH_EVENTS', {
	GetAuthSuccess: 'auth-success',
	GetAuthFailed: 'auth-failed',
	SessionTimeout: 'auth-session-timeout',
	LogoutSuccess: 'auth-logout-success',
	LicenseExpired: 'auth-license-expired'
});
/**
 * Session
 */
app.service('Session', function () {
	this.create = function (userId, currentUser,userAuth,userobj) {
		this.id = userId;
		this.user = currentUser;
		this.auth = userAuth;
        this.userobj = userobj;
	};
	this.destroy = function () {
		this.id = null;
		this.user = null;
		this.auth=null;
        this.userobj = null;
	};
	return this;
});
/**
 * TOKEN
 */
app.service('TOKEN', function ($cookies) {
	Object.assign(this,{
		key:'TOKEN',
		get(_default){
			let token = $cookies.get(this.key);
            if (!token || token.trim() === '') {
				if(_default){
                    token = _default;
				}
			}
			return token;
		},
		clear(){
			$cookies.remove(this.key);
		}
	});
	return this;
});
/**
 * CurrentUserId
 */
app.service('CURRENTUSERID', function ($cookies) {
    Object.assign(this,{
        key:'CURRENTUSERID',
        get(_default){
            let userid = $cookies.get(this.key);
            if (!userid || userid.trim() === '') {
                if(_default){
                    userid = _default;
                }
            }
            return userid;
        },
        clear(){
        $cookies.remove(this.key);
    }
});
return this;
});
/**
 * CURRENTUSER
 */
app.service('CURRENTUSER', function ($cookies) {
    Object.assign(this,{
        key:'CURRENTUSER',
        get(_default){
            let token = $cookies.get(this.key);
            if (!token || token.trim() === '') {
                if(_default){
                    token = _default;
                }
            }
            return token;
        },
        clear(){
        $cookies.remove(this.key);
    }
});
return this;
});