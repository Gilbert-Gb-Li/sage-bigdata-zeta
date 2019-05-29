# sage-bigdata-etl   

### 1．JDK安装与环境变量配置
    ① 使用命令 uname  –r 查看操作系统版本号；  
    ② 在ORACLE官网下载适合的JDK版本（建议8u45版本及以上）：  
        http://www.oracle.com/technetwork/java/javase/downloads  
    ③ 解压JDK版本，命令如下：  
       tar  -xvf  jdk-8u45-linux-x64.tar.gz  -C  /opt               //将tar.gz压缩包解压至opt  
    ④ 创建软连接： ln  -s  /opt/ jdk1.8.0_45  /opt/jdk  
    ⑤ 修改环境变量配置:  
       vi  /etc/profile                                 //编辑/etc/profile文件，加入下列命令  
          export  JAVA_HOME=/opt/jdk  
          export  CLASSPATH=.:$JAVA_HOME/lib/dt.jar:$JAVA_HOME/lib/tools.jar:$CLASSPATH  
          export  PATH=.:$JAVA_HOME/bin:$PATH  
    ⑥ 使环境变量立即生效： source  /etc/profile  

### 2．master安装与配置  
    ① 将sage-bigdata-etl-master压缩包放至opt下解压，命令如下：  
       unzip sage-bigdata-etl-master-3.0.0.zip  
    ② 进入解压后的目录将conf下所有带.template的文件去掉该后缀，命令如下：  
        cd  /opt/sage-bigdata-etl-master-3.0.0  
        mv  logback-master.xml.template   logback-master.xml  
        mv  master.conf.template   master.conf  
    ③ 修改master配置，hostname后指定mater的IP，path后指定db存放位置。  
        cd  /opt/sage-bigdata-etl-master-3.0.0/config  
        vi  master.conf  
        修改 master.store {  
                path=./db  
            }  
            master 绑定的主机,及跟worker通信的端口  
            actor.netty.tcp {  
                       hostname = "0.0.0.0" //0.0.0.0绑定到所有主机  
                       port=19091            //19091 默认绑定到的端口  
                     }  
            master 绑定的主机及对外开放的web端口  
            master.service.bind{  
                   ip=0.0.0.0 //0.0.0.0绑定到所有主机  
                   port=19090  //19090默认绑定到的端口  
                 }  
     ④. 启动与停止：  
         启动：cd  /opt/sage-bigdata-etl-master-3.0.0  
               bin/start.sh  
         停止：cd  /opt/sage-bigdata-etl-master-3.0.0  
               bin/stop.sh  

### 3．worker安装与配置
    ① 将sage-bigdata-etl-worker压缩包放至opt下解压，命令如下：  
       unzip sage-bigdata-etl-worker-3.0.0.zip  
    ② 进入解压后的目录将conf下所有带.template的文件去掉该后缀，命令如下：  
        cd  /opt/sage-bigdata-etl-worker-3.0.0  
        mv  logback-worker.xml.template   logback-worker.xml  
        mv  master.conf.template   master.conf  
    ③ 配置  
        app {  
            //唯一标示符,默认是worker 当时默认值时。系统启动会自动分配一个唯一的id  
          id = worker  
          #添加采集器信息到数据记录  
          add.collector.info = off  
          #添原始数据到数据记录  
          add.raw.data= on  
          #添加采集时间到数据记录  
          add.data.receive.time= off  
          remote {  
            //配置master的地址  
            host = 127.0.0.1  
            port = 19091  
            type = master  
          }
          process {  
            #处理程序数据读取线程（针对数据源是文件（HDFS，FTP，本地目录），表示一次读取几个文件）  
            size = 1  
            #处理程序最少等待缓存的数据条数，当数据缓存达到这个数值处理程序将等待，直到缓存数据低于这个值  
            cache.size = 20  
            //    #处理程序最多使用解析器的个数  
            lexer.size = 1  
            #每一个处理程序写writer的并发数  
            writer.size = 1  
            #每一个writer刷新缓存的时间 单位s  
            writer.flush.times=60  
            #处理程序一次等待的时间  
            wait.times = 500  
          }  
          #数据存储使用的方式，暂时使用derby  
          store {  
            path = ./db  
            position {  
              class = com.haima.sage.bigdata.etl.store.position.DerbyReadPositionStore  

              # [positive , negative]  
              # positive 积极模式 每读取一个offset 及时 记录文件读取位置（掉电丢失部分数据）  
              # negative 消极模式 等待当前缓存写完成 后再记录当前位置（掉电后可能重复读部分数据）  
              flush = negative  
              #多少条记录一次文件读取的位置  
              offset = 1000  
            }  
     ④. 启动与停止：  
         启动：cd  /opt/sage-bigdata-etl-worker-3.0.0  
               bin/start.sh  
         停止：cd  /opt/sage-bigdata-etl-worker-3.0.0  
               bin/stop.sh  

### 4. 内存配置  
    修改bin/sage-bigdata-etl.sh  
    function start() {  
       security_file=$conf/security.keygen  
        if [ ! -f "$security_file" ]; then  
        touch "$security_file"  
        fi 
        // 这里指定启动的内存配置,最大1G 最小128M  
        java  -Xms128M -Xmx1G -Doracle.jdbc.J2EE13Compliant=true -cp $conf:$cp $main $params > $out 2>&1 &  
        echo $! > $pid  
    }  

### 5. 版本发布  
    建立发布的分支或标签，使用 maven 命令：mvn versions:set -DnewVersion=1.2.3-[rc1|rc2|GM] 标识发布版本  