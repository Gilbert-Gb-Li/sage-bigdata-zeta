How to build
=============================
编译 release 包或一键安装 Zip 版本
-----------

```sh
# 编译并产生 release包
mvn clean package -DskipTest

or
# 编译一键安装包
 mvn clean assembly:assembly -DskipTests

```
