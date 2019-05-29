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

package porter.store

import org.scalatest.FunSuite
import org.scalatest.Matchers
import scala.concurrent.Await
import scala.concurrent.duration._
import  com.haima.sage.bigdata.etl.common.Constants.executor
import porter.model._
import porter.model.PropertyList.mutableSource
import java.io._

class PropertiesStoreTest extends FunSuite with Matchers {

  val testPw = Password("123456")
  val readOnly = mutableSource.toFalse
  val acc = Account("admin", readOnly(Map("email" -> "admin@dasd.com", "enabled" -> "true")), Set("users", "admin"), Seq(testPw))

  private def createProps() = {
    val props = new java.util.Properties()
    props.setProperty("porter.realms", "zeta")
    props.setProperty("porter.zeta.name", "zeta")
    props.setProperty("porter.zeta.accounts", "admin")

    props.setProperty("porter.zeta.account.admin.secret", testPw.asString)
    props.setProperty("porter.zeta.account.admin.groups", "admin, users")
    props.setProperty("porter.zeta.account.admin.props", "email -> admin@raysdata.com, enabled->true")
    props.setProperty("porter.zeta.groups", " admin,  users ")
    props.setProperty("porter.zeta.group.admin.rules", "resource:*:/**, base:manage")
    val stream=new FileOutputStream("D:\\raysdata_gitlab\\zeta\\db\\store.properties")
    props.store(stream,"Copyright (c) haima ")
    stream.close()

    props
  }

  private def createProps(values: Map[String, String]) = {
    val props = new java.util.Properties()
    for ((k, v) <- values) props.setProperty(k, v)
    props
  }

  test("list realms") {
    val store = PropertiesStore(createProps(Map(
      "porter.realms" -> "zeta,app2,app3",
      "porter.zeta.name" -> "A realm 1",
      "porter.app2.name" -> "A realm 2",
      "porter.app3.name" -> "A realm 3",
      "porter.app4.name" -> "A realm 4"
    )))
    Await.result(store.allRealms, 5.seconds) should be (List(Realm("zeta", "A realm 1"),
      Realm("app2", "A realm 2"), Realm("app3", "A realm 3")))
  }

  test("find realms") {
    val store = PropertiesStore(createProps())
    Await.result(store.findRealms(Set("zeta")), 5.seconds) should be (List(Realm("zeta", "zeta")))
    Await.result(store.findRealms(Set("asdasd")), 5.seconds) should be (List())
  }

  test("list groups") {
    val store = PropertiesStore(createProps())
    Await.result(store.allGroups("zeta"), 5.seconds) should be (List(
      Group("admin", readOnly(Map.empty), Set("resource:read:/main/**", "base:manage")), Group("users", readOnly(Map.empty))))
  }

  test ("find groups") {
    val store = PropertiesStore(createProps())
    Await.result(store.findGroups("zeta", Set("users")), 5.seconds) should be (List(Group("users", readOnly(Map.empty))))
  }

  test ("list accounts") {
    val store = PropertiesStore(createProps())
    Await.result(store.allAccounts("zeta"), 5.seconds) should be (List(acc))
  }

  test ("find accounts") {
    val store = PropertiesStore(createProps())
    Await.result(store.findAccounts("zeta", Set("admin")), 5.seconds) should be (List(acc))
  }

  test ("find accounts with credentials") {
    val store = PropertiesStore(createProps())
    val creds: Credentials = PasswordCredentials("admin", "bla")
    Await.result(store.findAccountsFor("zeta", Set(creds)), 5.seconds) should be (List(acc))
  }
}
