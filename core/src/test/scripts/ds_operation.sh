#!/usr/bin/env bash
# collector all
curl -XGET localhost:19090/config/collector;
 echo "";
curl -XGET localhost:19090/config/collector/3adfd664-a24d-42e2-ada4-0f765febdd77;
 echo "";
 curl -XGET localhost:19090/config/parser
 echo "";
 curl -XGET localhost:19090/metrics/collectorId/dataSourceId
curl -XGET localhost:19090/health/3adfd664-a24d-42e2-ada4-0f765febdd77

# add datasource
for ((i=0;i<=10;i++))
    do
       curl -XPOST localhost:19090/config/datasource -d '{"collector":{"id":"3adfd664-a24d-42e2-ada4-0f765febdd77","host":"localhost","port":19093,"cache":0},"state":"CREATE","parser":{"delimit":" ","name":"delimitWithKeyMap"},"writers":[{"name":"es","hostPorts":[["yzh",9300]],"esType":"H3C","cache":1,"cluster":"yzh_es","id":"1","index":"logs_%{yyyyMMdd}"}],"dataSource":{"name":"net","host":null,"protoType":"syslog","cache":10000,"port":4300,"contentType":"syslog","properties":null,"metadata":{"host":"0.0.0.0","port":4303,"app":"h3c_4303","data":[]},"codec":{"delimit":"\r\n","name":"delimit"},"protocol":null},"id":"h3c_'${i}'"}';
         echo "";
    done
    # update datasource
for ((i=0;i<=10;i++))
    do
       curl -XPUT localhost:19090/config/datasource -d '{"collector":{"id":"3adfd664-a24d-42e2-ada4-0f765febdd77","host":"localhost","port":19093,"cache":0},"state":"CREATE","parser":{"delimit":" ","name":"delimitWithKeyMap"},"writers":[{"name":"es","hostPorts":[["yzh",9300]],"esType":"H3C","cache":1,"cluster":"yzh_es","id":"1","index":"logs_%{yyyyMMdd}"}],"dataSource":{"name":"net","host":null,"protoType":"syslog","cache":10000,"port":4300,"contentType":"syslog","properties":null,"metadata":{"host":"0.0.0.0","port":4303,"app":"h3c_4303","data":[]},"codec":{"delimit":"\r\n","name":"delimit"},"protocol":null},"id":"h3c_'${i}'"}';
         echo "";
    done

   # get datasource
for ((i=0;i<=10;i++))
    do
       curl -XGET localhost:19090/config/datasource/h3c_${i};
         echo "";
    done
# get all datasource
curl -XGET localhost:19090/config/datasource;
 echo "";
# get stop datasource
for ((i=0;i<=10;i++))
    do
        curl -XPUT local:19090/operation/stop/h3c_${i};
        echo "";
    done
 # get start datasource
for ((i=0;i<=10;i++))
    do
        curl -XPUT local:19090/operation/start/h3c_${i};
        echo "";
    done
      # delete datasource
for ((i=0;i<=10;i++))
    do
       curl -XDELETE localhost:19090/config/datasource/h3c_${i};
         echo "";
    done