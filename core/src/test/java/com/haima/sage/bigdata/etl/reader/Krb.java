package com.haima.sage.bigdata.etl.reader;

import com.sun.security.auth.module.Krb5LoginModule;

import javax.security.auth.Subject;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class Krb {
    private void loginImpl(final String propertiesFileName) throws Exception {
        System.out.println("NB: system property to specify the krb5 config: [java.security.krb5.conf]");
        //System.setProperty("java.security.krb5.conf", "/etc/krb5.conf");

        System.out.println(System.getProperty("java.version"));

        System.setProperty("sun.security.krb5.conf", "krb5.conf");
        System.setProperty("sun.security.krb5.debug", "true");

        final Subject subject = new Subject();

        final Krb5LoginModule krb5LoginModule = new Krb5LoginModule();
        final Map<String, String> optionMap;
        optionMap = new HashMap<String, String>();

        if (propertiesFileName == null) {
            //  optionMap.put("ticketCache", "krb5.conf");
            optionMap.put("keyTab", "kerberos.keytab");
            optionMap.put("principal", "root/kunlun"); // default realm

            optionMap.put("doNotPrompt", "true");
            optionMap.put("refreshKrb5Config", "true");
            optionMap.put("useTicketCache", "true");
            optionMap.put("renewTGT", "true");
            optionMap.put("useKeyTab", "true");
            optionMap.put("storeKey", "true");
            optionMap.put("isInitiator", "true");
        } else {
            File f = new File(propertiesFileName);
            System.out.println("======= loading property file [" + f.getAbsolutePath() + "]");
            try (InputStream is = new FileInputStream(f)) {
                Properties p = new Properties();
                p.load(is);
                for (Map.Entry<Object, Object> entry : p.entrySet()) {
                    optionMap.put(entry.getKey().toString(), entry.getValue().toString());
                }

            }

        }
        optionMap.put("debug", "true"); // switch on debug of the Java implementation

        krb5LoginModule.initialize(subject, null, new HashMap<String, String>(), optionMap);

        boolean loginOk = krb5LoginModule.login();
        System.out.println("======= login:  " + loginOk);

        boolean commitOk = krb5LoginModule.commit();
        System.out.println("======= commit: " + commitOk);
        System.out.println("======= Subject: " + subject);
    }

    public static void main(String[] args) throws Exception {
        System.out.println("A property file with the login context can be specified as the 1st and the only paramater.");
        final Krb krb = new Krb();
        krb.loginImpl(args.length == 0 ? null : args[0]);
    }
}
/*
创建一个配置文件krb5.properties：

        keyTab=/etc/hadoop/conf/hdfs.keytab
        principal=hdfs/cdh1@JAVACHEN.COM

doNotPrompt=true
        refreshKrb5Config=true
        useTicketCache=true
        renewTGT=true
        useKeyTab=true
        storeKey=true
        isInitiator=true*/
