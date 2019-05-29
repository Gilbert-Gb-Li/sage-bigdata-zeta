package com.haima.sage.bigdata.etl.utils

import java.util.regex.Pattern

/**
  * Created by zhhuiyan on 15/5/20.
  */


trait WildMatch extends Logger {


  private val escape = '\\'
  val normals = Array('$', '^', '(', ')', '|', '.')
  val spacial = Array('*', '?', '{', '}')

  def wildMatch(pattern: Pattern, str: String): Boolean = {
    pattern.matcher(str).matches
  }

  def wildMatch(pattern: Array[Char], str: String): Boolean = {
    toPattern(pattern).matcher(str).matches
  }


  def toPattern(patterns: Array[Char]): Pattern = {
    var result: String = "^"
    var brace: Boolean = false


    var i: Int = 0
    while (i < patterns.length) {

      val ch: Char = patterns(i)
      var isMeta: Boolean = false
      if (ch == escape) {
        if (spacial.contains(patterns(i + 1))) {
          i += 1
          result += escape + patterns(i)
        }
        else {
          result += escape + ch
        }
      }
      else {
        if (normals.contains(ch)) {
          result += s"$escape$ch"
          isMeta = true
        }
        if (!isMeta) {
          if (ch == '*') {
            result += ".*"
          }
          else if (ch == '?') {
            result += ".?"
          }
          else {
            if (ch == '{') {
              brace = true
              result += "("
            }
            else if (ch == '}') {
              brace = false
              result += ")"
            }
            else if (brace && ch == ',') {
              result += '|'
            }
            else {
              result += ch
            }
          }
        }
      }

      i += 1
    }
    Pattern.compile(result + "$")
  }

}

