/*
 * Copyright 2014 porter <https://github.com/eikek/porter>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package porter.app.client.spray

import java.util.concurrent.ExecutionException

import akka.actor.ActorRef
import akka.http.scaladsl.model.{DateTime, HttpMethods, StatusCodes, Uri}
import akka.http.scaladsl.model.headers._
import akka.http.scaladsl.server.AuthenticationFailedRejection.CredentialsRejected
import akka.http.scaladsl.server.{AuthenticationFailedRejection, Directive0, Directive1, Route}
import akka.http.scaladsl.server.Directives._
import akka.util.Timeout
import porter.app.akka.PorterUtil
import porter.app.client.PorterContext
import porter.auth.AuthResult
import porter.client.messages._
import porter.model.{Account, _}

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.util.control.NonFatal
import scala.util.{Failure, Success}

trait PorterDirectives {

  private def authorizationHeader: Directive1[Option[Authorization]] = extract { ctx =>
    ctx.request.header[`Authorization`]
  }

  implicit class CredentialsConcat(directive: Directive1[Set[Credentials]]) {
    /**
     * Concatenates the credentials into one set
     * @param other
     * @return
     */
    def ++ (other: Directive1[Set[Credentials]]): Directive1[Set[Credentials]] =
      for (c1 <- directive; c2 <- other) yield c1 ++ c2

    /**
     * Passes if there are no credentials, rejected otherwise
     * @return
     */
    def isEmpty: Directive0 = directive.flatMap {
      case s if s.nonEmpty => reject()
      case _ => pass
    }
  }

  /**
   * Retrieves credentials from http form data using fields "porter.username" and
   * "porter.password".
   *
   * @return
   */
  def formCredentials(usernameField: String = "porter.username", passwordField: String = "porter.password"): Directive1[Set[Credentials]] =
    (for {
      un <- formField(usernameField)
      pw <- formField(passwordField)
    } yield Set[Credentials](PasswordCredentials(un, pw))).recover(_ => provide(Set.empty))

  /**
   * Retrieves credentials from a cookie.
   *
   * @param cookieKey
   * @param cookieName
   * @return
   */
  def cookieCredentials(cookieKey: Vector[Byte], cookieName: String = "PORTER"): Directive1[Set[Credentials]] =
    optionalCookie(cookieName).map {
      case Some(c) =>
        val derived = DerivedCredentials.tryDecode(cookieKey, c.value).toOption
        derived.map(Set[Credentials](_)).getOrElse(Set.empty)
      case _ => Set.empty[Credentials]
    }

  /**
   * Retrieves http basic credentials from `Authorization` header.
   * @return
   */
  def basicCredentials: Directive1[Set[Credentials]] = {
    extractCredentials.map {
      case Some(BasicHttpCredentials(user, pass)) => Set[Credentials](PasswordCredentials(user, pass))
      case _ => Set.empty[Credentials]
    }
  }

  /**
   * Retrieves http digest credentials from `Authorization` header.
   *
   * @return
   */
//  def digestCredentials: Directive1[Set[Credentials]] = extractRequest.flatMap { request =>
//    extractCredentials.flatMap {
//      case Some(HttpDigestCredentials(creds, request)) => provide(Set[Credentials](dcred))
//      case _ => provide(Set.empty)
//    }
//  }

  /**
   * Retrieves credentials from either form data, `Authorization` header
   * (http basic and digest) or a cookie.
   *
   * @param cookieKey
   * @param cookieName
   * @return
   */
  def allCredentials(cookieKey: Vector[Byte], cookieName: String = "PORTER"): Directive1[Set[Credentials]] =
    formCredentials() ++ cookieCredentials(cookieKey, cookieName) ++ basicCredentials // ++ digestCredentials

  /**
   * Rejects the request, if there is at least one credentials supplied with the request.
   *
   * @param cookieKey
   * @param cookieName
   * @return
   */
  def noCredentials(cookieKey: Vector[Byte], cookieName: String = "PORTER"): Directive0 =
    allCredentials(cookieKey, cookieName).isEmpty

  /**
   * Sets a cookie to authenticate the given account. The account must have at least one secret. The
   * cookie data is encrypted with AES using the given `cookieKey`. This key must be the same
   * (obviously) when retrieving the cookie credentials. You can generate such a key with
   * [[porter.util.AES.generateRandomKey]].
   *
   * If `maxAge` is specified a persistent cookie is created that is valid the given time. Otherwise a
   * session cookie is created where the cookie data is valid `sessionAge`. If `sessionAge` is not
   * specified, the session is valid for 1 day.
   *
   * @param account
   * @param cookieSettings
   * @param selectSecret a function to select a secret from the account that should be encoded
   *                     in the cookie
   * @return
   */
  def setPorterCookie(account: Account, cookieSettings: CookieSettings, selectSecret: Account => Secret = _.secrets.head): Directive0 = {
    val valid = cookieSettings.persistAge.orElse(cookieSettings.sessionAge).getOrElse(1.days)
    val data = DerivedCredentials(account.name, selectSecret(account), valid).encode(cookieSettings.cookieKey)
    setCookie(HttpCookie(name = cookieSettings.cookieName,
      value = data,
      maxAge = cookieSettings.persistAge.map(_.toSeconds),
      expires = cookieSettings.persistAge.map(a => DateTime.now + a.toMillis),
      secure = cookieSettings.cookieSecure,
      httpOnly = true,
      path = Some(cookieSettings.cookiePath)))
  }

  /**
   * Authenticates the user at the given porter actor and provides the associated [[porter.model.Account]]
   * if successful. Otherwise the route is rejected.
   *
   * @param pctx
   * @param creds
   * @param ec
   * @param timeout
   * @return
   */
  def authenticateAccount(pctx: PorterContext, creds: Set[Credentials])
                         (implicit ec: ExecutionContext, timeout: Timeout): Directive1[Account] =
    onComplete(PorterUtil.authenticateAccount(pctx.porterRef, pctx.realm, creds, pctx.decider)).flatMap {
      case Success((_, account)) => provide(account)
      case Failure(x) => x match {
        case ee: ExecutionException =>
          ee.getCause match {
            case NonFatal(_) => reject()
            case t => throw t
          }
        case NonFatal(_) => reject()
        case e => throw e
      }
    }

  /**
   * Authenticates the user at the given porter actor and provides the [[porter.auth.AuthResult]]
   * if successful. Otherwise the route is rejected.
   *
   * @param pctx
   * @param creds
   * @param ec
   * @param timeout
   * @return
   */
  def authenticateResult(pctx: PorterContext, creds: Set[Credentials])
                        (implicit ec: ExecutionContext, timeout: Timeout): Directive1[AuthResult] =
    onSuccess(PorterUtil.authenticationFuture(pctx.porterRef, pctx.realm, creds, pctx.decider)).flatMap {
      case AuthenticateResp(Some(r)) => provide(r)
      case AuthenticateResp(_) => reject()
    }

  /**
   * Simple "best effort" that checks the `User-Agent` header value to determine whether the client
   * is a web browser or something else.
   *
   * If the header value is present and contains one of "presto", "chrome", "opera", "gecko" or "webkit"
   * it is considered to be a browser.
   *
   * @return
   */
  def userAgentIsBrowser: Directive0 = headerValueByName(`User-Agent`.name).flatMap { agent =>
    val agentlc = agent.toLowerCase
    def contains(s: String*): Boolean = s.foldLeft(false) { (r, s) =>
      if (r) true else agentlc.contains(s)
    }
    if (contains("crawler", "bot")) reject()
    else if (contains("chrome", "opera", "gecko", "webkit", "presto")) pass
    else reject()
  }

  /**
   * A rejection that adds a `WWW-Authenticate` header to the response to request basic
   * authentication from the client.
   *
   * @param cause
   * @param realm
   * @return
   */
  def httpBasicChallenge(cause: AuthenticationFailedRejection.Cause = CredentialsRejected, realm: String = "Protected Area") =
    AuthenticationFailedRejection(cause, HttpChallenge("Basic", realm, Map.empty[String, String]))

  /**
   * A rejection that adds a `WWW-Authenticate` header to the response to request
   * http digest authentication from the client.
   *
   * @param porterRef
   * @param cause
   * @param realm
   * @param uri
   * @return
   */
  def sendHttpDigestChallenge(porterRef: ActorRef, cause: AuthenticationFailedRejection.Cause = CredentialsRejected, realm: String = "Protected Area", uri: Uri)
                         (implicit ec: ExecutionContext, timeout: Timeout): Route = {
    import akka.pattern.ask
    val f = (porterRef ? RetrieveServerNonce).mapTo[RetrieveServerNonceResp]
    onSuccess(f) { resp =>
      val params = Map(
        "nonce" -> resp.nonce,
        "qop" -> "auth,auth-int"
      )
      reject(AuthenticationFailedRejection(cause, HttpChallenge("Digest", realm, params)))
    }
  }

  /**
   * Completes the route by sending a "challenge" to the client to request authentication. If
   * `loginPage` is specified and the client is likely to be a browser, the client is
   * redirected to this page.
   * If the request is not likely to be a browser request, the route is rejected with a http
   * basic `WWW-Authenticate` response header.
   *
   * @param loginPage
   * @param realm
   * @return
   */
  def sendChallenge(cause: AuthenticationFailedRejection.Cause = CredentialsRejected, loginPage: Option[Uri] = None, realm: String = "Protected Area"): Route = {
    val redirectUrl: Directive1[Uri] = loginPage.map(provide).getOrElse(reject())
    redirectUrl { url =>
      userAgentIsBrowser {
        redirect(url, StatusCodes.TemporaryRedirect)
      }
    } ~
    reject(httpBasicChallenge(cause, realm))
  }

  /**
   * Loads the account with the given id.
   *
   * @param porterContext
   * @param account
   * @return
   */
  def loadAccount(porterContext: PorterContext, account: Ident)(implicit ec: ExecutionContext, to: Timeout): Directive1[Account] = {
    onSuccess(PorterUtil.findAccount(porterContext.porterRef, porterContext.realm, account)).flatMap {
      case Some(a) => provide(a)
      case _ => reject()
    }
  }

  /**
   * Checks the given permissions against the policy of the given account. If it is successful,
   * the route passes, otherwise it is rejected.
   *
   * @param porterContext
   * @param account
   * @param perms
   * @return
   */
  def authz(porterContext: PorterContext, account: Ident, perms: Set[String])
           (implicit ec: ExecutionContext, timeout: Timeout): Directive0 = {
    onSuccess(PorterUtil.authorize(porterContext.porterRef, porterContext.realm, account, perms)).flatMap {
      value => authorize(value)
    }
  }

  /**
   * Checks if the given account has the apropriate permissions to access the current resource.
   *
   * @param porterContext
   * @param account
   * @return
   */
  def authzResource(porterContext: PorterContext, account: Ident)
                   (implicit ec: ExecutionContext, timeout: Timeout): Directive0 =
    resourcePermission.flatMap { perm =>
      authz(porterContext, account, Set(perm))
    }

  /**
   * Extract the permission for the current uri path from the request. It creates
   * a permission string for the `ResourcePermission`, that looks like this
   * {{{
   *   resource:[read|write|delete]:request.uri.path
   * }}}
   * The action is determined by the http method.
   *
   * @return
   */
  def resourcePermission: Directive1[String] = {
    extract(ctx => ctx.request.method match {
      case HttpMethods.DELETE => "resource:delete:"+ ctx.request.uri.path
      case x if x.isSafe => "resource:read:"+ ctx.request.uri.path
      case _ => "resource:write:"+ ctx.request.uri.path
    })
  }
}

object PorterDirectives extends PorterDirectives