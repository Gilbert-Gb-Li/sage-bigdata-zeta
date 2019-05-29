#!/bin/sh -
BASE="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
JDBC=$BASE/jdbc
mvn install:install-file -Dfile=$JDBC/ojdbc6-11.2.0.4.jar -DgroupId=com.oracle -DartifactId=ojdbc6 -Dversion=11.2.0.4 -Dpackaging=jar
mvn install:install-file -Dfile=$JDBC/xdb6-11.2.0.4.jar -DgroupId=com.oracle -DartifactId=xdb6 -Dversion=11.2.0.4 -Dpackaging=jar
mvn install:install-file -Dfile=$JDBC/xmlparserv2.jar -DgroupId=com.oracle -DartifactId=xmlparserv2 -Dversion=10.2.0.2 -Dpackaging=jar

mvn install:install-file -Dfile=$BASE/zkclient/zkclient-0.3.0-cw.jar -DgroupId=com.101tec -DartifactId=zkclient -Dversion=0.3 -Dpackaging=jar

mvn install:install-file -Dfile=$JDBC/sqljdbc4.jar -DgroupId=com.microsoft.sqlserver -DartifactId=sqljdbc4 -Dversion=4.0 -Dpackaging=jar
mvn install:install-file -Dfile=$JDBC/sqljdbc.jar -DgroupId=com.microsoft.sqlserver -DartifactId=sqljdbc -Dversion=4.0 -Dpackaging=jar

mvn install:install-file -Dfile=$JDBC/db2jcc4-4.9.78.jar -DgroupId=com.ibm.db2.jcc -DartifactId=db2jcc4 -Dversion=4.9.78 -Dpackaging=jar
mvn install:install-file -Dfile=$JDBC/db2jcc4-4.22.29.jar -DgroupId=com.ibm.db2.jcc -DartifactId=db2jcc4 -Dversion=4.22.29 -Dpackaging=jar

mvn install:install-file -Dfile=$BASE/ansj_seg/ansj_seg-2.0.8.jar -DgroupId=org.ansj -DartifactId=ansj_seg -Dversion=2.0.8 -Dpackaging=jar
mvn install:install-file -Dfile=$BASE/snmp/snmp4j-smi.jar -DgroupId=org.snmp4j -DartifactId=snmp4j-smi -Dversion=1.2.0 -Dpackaging=jar
mvn install:install-file -Dfile=$BASE/snmp/snmp6_1.jar -DgroupId=uk.co.westhawk -DartifactId=snmp -Dversion=6.1 -Dpackaging=jar
mvn install:install-file -Dfile=$JDBC/phoenix-core-4.8.0-cdh5.8.0.jar -DgroupId=org.apache.phoenix -DartifactId=phoenix-core -Dversion=4.8.0-cdh5.8.0 -Dpackaging=jar
mvn install:install-file -Dfile=$JDBC/phoenix-core-4.8.0-cdh5.8.0.pom -DgroupId=org.apache.phoenix -DartifactId=phoenix-core -Dversion=4.8.0-cdh5.8.0 -Dpackaging=pom
mvn install:install-file -Dfile=$JDBC/phoenix-4.8.0-cdh5.8.0.pom -DgroupId=org.apache.phoenix -DartifactId=phoenix -Dversion=4.8.0-cdh5.8.0 -Dpackaging=pom
mvn install:install-file -Dfile=$JDBC/driver.sysbase-3.0.jar -DgroupId=com.sybase.jdbc3.jdbc -DartifactId=SybDriver -Dversion=0.1.0 -Dpackaging=jar