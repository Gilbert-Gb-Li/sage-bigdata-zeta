@echo off
call mvn install:install-file -Dfile=%cd%\jdbc\ojdbc6-11.2.0.4.jar -DgroupId=com.oracle -DartifactId=ojdbc6 -Dversion=11.2.0.4 -Dpackaging=jar
call mvn install:install-file -Dfile=%cd%\jdbc\xdb6-11.2.0.4.jar -DgroupId=com.oracle -DartifactId=xdb6 -Dversion=11.2.0.4 -Dpackaging=jar
call mvn install:install-file -Dfile=%cd%\jdbc\xmlparserv2.jar -DgroupId=com.oracle -DartifactId=xmlparserv2 -Dversion=10.2.0.2 -Dpackaging=jar
call mvn install:install-file -Dfile=%cd%\jdbc\sqljdbc4.jar -DgroupId=com.microsoft.sqlserver -DartifactId=sqljdbc4 -Dversion=4.0 -Dpackaging=jar
call mvn install:install-file -Dfile=%cd%\jdbc\db2jcc4-4.9.78.jar -DgroupId=com.ibm.db2.jcc -DartifactId=db2jcc4 -Dversion=4.9.78 -Dpackaging=jar
call mvn install:install-file -Dfile=%cd%\jdbc\db2jcc4-4.22.29.jar -DgroupId=com.ibm.db2.jcc -DartifactId=db2jcc4 -Dversion=4.22.29 -Dpackaging=jar
call mvn install:install-file -Dfile=%cd%\jdbc\sqljdbc.jar -DgroupId=com.microsoft.sqlserver -DartifactId=sqljdbc -Dversion=4.0 -Dpackaging=jar
call mvn install:install-file -Dfile=%cd%\ansj_seg\ansj_seg-2.0.8.jar -DgroupId=org.ansj -DartifactId=ansj_seg -Dversion=2.0.8 -Dpackaging=jar
call mvn install:install-file -Dfile=%cd%\snmp\snmp6_1.jar -DgroupId=uk.co.westhawk -DartifactId=snmp -Dversion=6.1 -Dpackaging=jar
call mvn install:install-file -Dfile=%cd%\jdbc\phoenix-core-4.8.0-cdh5.8.0.jar -DgroupId=org.apache.phoenix -DartifactId=phoenix-core -Dversion=4.8.0-cdh5.8.0 -Dpackaging=jar
call mvn install:install-file -Dfile=%cd%\jdbc\phoenix-core-4.8.0-cdh5.8.0.pom -DgroupId=org.apache.phoenix -DartifactId=phoenix-core -Dversion=4.8.0-cdh5.8.0 -Dpackaging=pom
call mvn install:install-file -Dfile=%cd%\jdbc\phoenix-4.8.0-cdh5.8.0.pom -DgroupId=org.apache.phoenix -DartifactId=phoenix -Dversion=4.8.0-cdh5.8.0 -Dpackaging=pom
call mvn install:install-file -Dfile=%cd%\jdbc\driver.sysbase-3.0.jar -DgroupId=com.sybase.jdbc3.jdbc -DartifactId=SybDriver -Dversion=0.1.0 -Dpackaging=jar