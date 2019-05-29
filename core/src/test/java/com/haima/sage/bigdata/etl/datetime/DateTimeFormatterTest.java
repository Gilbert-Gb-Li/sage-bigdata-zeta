package com.haima.sage.bigdata.etl.datetime;


import com.haima.sage.bigdata.etl.normalization.format.LocalDateTranslator;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.junit.Assert;
import org.junit.Test;

import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

/**
 * Created: 2016-06-06 13:11.
 * Author:  Zhao
 * Email:   guangzhao_jia@sage.com
 */
public class DateTimeFormatterTest {

    @Test
    public void test001() {
        String strDate = "2016-06-06 13:20:22";
        DateTimeFormatter dateTimeFormatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");
        DateTime dateTime = dateTimeFormatter.parseDateTime(strDate);
        Assert.assertEquals("测试时间是否相等", strDate, dateTime.toString(dateTimeFormatter));
    }

    @Test
    public void testZone() {

        String date = "22/Mar/2016:22:30:01";
        SimpleDateFormat format = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss", Locale.ENGLISH);
        try {
            System.out.println("format = " + format.parse(date));
        } catch (ParseException e) {
            e.printStackTrace();
        }


    }


    /*LocalDateTranslator(f)*/
    @Test
    public void testformat() {
        LocalDateTranslator format = LocalDateTranslator.apply("yyyyMM-dd");
        System.out.println("format = " + format.parse("19861011"));

    }

    @Test
    public void test002() {
        Date date = new Timestamp(System.currentTimeMillis());
        DateTime dateTime = new DateTime(date.getTime());
    }
}
