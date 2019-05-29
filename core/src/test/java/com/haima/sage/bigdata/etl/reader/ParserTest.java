package com.haima.sage.bigdata.etl.reader;

import com.haima.sage.bigdata.etl.common.model.Delimit;
import com.haima.sage.bigdata.etl.common.model.Regex;
import com.haima.sage.bigdata.etl.lexer.DelimiterLexer;
import com.haima.sage.bigdata.etl.lexer.RegexLexer;
import scala.Option;
import scala.Some;
import scala.Tuple3;
import scala.collection.immutable.List;

/**
 * Created by zhhuiyan on 15/11/5.
 */
public class ParserTest {

    public static void main(String[] args) throws Exception {
        String log = "user-PC [事件日志]事件类型: 扫描病毒\n事件级别: 提示信息\n事件信息: 1：开始时间2015-05-08 14:34:39，结束时间2015-05-08 15:19:38，扫描文件194922个，发现染毒病毒文件24个\n计算机名称: user-PC\nIP: 10.88.132.156\n报告者:RavService\n发现日期: 2015-05-08 15:19:38\n ";
        Delimit delimit = new Delimit(new Some<String>(" "), "a,b,c".split(","), null, Option.<List<Tuple3<String, String, String>>>empty());
        DelimiterLexer.Lexer parser = DelimiterLexer.apply(delimit);
        System.out.println("parser = " + parser.parse("1 2 3"));

        Regex regex = new Regex("%{NOTSPACE:computer} %{NOTSPACE}?:\\s*%{NOTSPACE:event_type}\\n%{NOTSPACE}?:\\s*%{NOTSPACE:event_level}\\n%{NOTSPACE}?:\\s*%{DATA:event_info}\\n%{NOTSPACE}?:\\s*%{NOTSPACE:computer_name}\\n%{NOTSPACE}?:\\s*%{IP:ipaddress}\\n%{NOTSPACE}:\\s*%{NOTSPACE:reportor}\\n%{NOTSPACE}?:\\s*%{DATE:date} %{TIME:time}\\n\\s*",
                null, Option.<List<Tuple3<String, String, String>>>empty());
        RegexLexer.Lexer parserG = RegexLexer.apply(regex);
        System.out.println("parser = " + parserG.parse(log));
    }
}
