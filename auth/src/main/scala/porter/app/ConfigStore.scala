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

package porter.app

import porter.store.SimpleStore
import com.typesafe.config.Config
import scala.util.Try

/**
 * Example:
 * {{{
 *   cfg {
 *     realm1: {
 *       name: "my great realm"
 *       groups: {
 *         admin: {
 *           rules: [ "bla.bla", "!bla.bla.blop" ]
 *           props: {
 *             key: "value"
 *           }
 *         }
 *         ...
 *       }
 *       accounts: {
 *         john: {
 *           secret: "cryptedstring"
 *           groups: [ "admin", "user" ]
 *           props: {
 *             key: "value"
 *           }
 *         }
 *       }
 *     }
 *   }
 * }}}
 *
 */
class ConfigStore(cfg: Config) extends SimpleStore {
  import collection.JavaConversions._
  import porter.model._

  val realms = (for (k <- cfg.root().keySet())
    yield Realm(k, cfg.getString(s"$k.name"))).toList

  val groups = for {
      r <- realms
      g <- cfg.getConfig(s"${r.id.name}.groups").root().keySet()
    } yield r -> Group(
      g,
      Try(getProps(cfg.getConfig(s"${r.id.name}.groups.$g.props"))).getOrElse(Map.empty),
      cfg.getStringList(s"${r.id.name}.groups.$g.rules").toSet
    )

  val accounts = for {
      r <- realms
      a <- cfg.getConfig(s"${r.id.name}.accounts").root().keySet()
    } yield r -> Account(
      a,
      Try(getProps(cfg.getConfig(s"${r.id.name}.accounts.$a.props"))).getOrElse(Map.empty),
      cfg.getStringList(s"${r.id.name}.accounts.$a.groups").map(Ident.apply).toSet,
      Seq(Password.crypted(cfg.getString(s"${r.id.name}.accounts.$a.secret")))
    )

  private def getProps(cfg: Config): Properties = {
    val props = (for (k <- cfg.root().keySet()) yield k -> cfg.getString(k)).toMap
    PropertyList.mutableSource.toFalse(props)
  }


}
