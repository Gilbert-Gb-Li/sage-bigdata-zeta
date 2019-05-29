SNMP4J-SMI README
=================

SNMP4J-SMI provides an API to use MIB specification data with SNMP4J and other
applications using the interfaces org.snmp4j.util.OIDTextFormat and
org.snmp4j.util.VariableTextFormat.

For usage information see the package API documentation of com.snmp4j.smi at:
http://www.snmp4j.org/smi/doc/com/snmp4j/smi/package-summary.html

For the impatient, the class to start with is "com.snmp4j.smi.SmiManager".

There is a fully functional sample application which can walk a sub-tree
of a SNMP entity (agent) and manage a MIB repository for SNMP4J-SMI.

To compile this example, first change your working directory to the
"examples" directory and then run:

  javac -cp ../snmp4j-smi.jar;../lib/snmp4j-2.2.0.jar SnmpWalk.java

The example program is executed with the following command line to get
its usage help:

  java -cp ../snmp4j-smi.jar;../lib/snmp4j-2.2.0.jar;. SnmpWalk help all

For more information on SNMP4J-SMI see the FAQ at:
http://oosnmp.net/confluence/display/SNMP4J/SNMP4J-SMI+FAQ

Using SNMP4J-SMI is subject to the SNMP4J-SMI license agreement which
is included with the sources and downloadable from:
http://www.snmp4j.org/smi/LICENSE.txt

To purchase a full version license of SNMP4J-SMI please visit:
http://www.snmp4j.org/html/howtobuy.html