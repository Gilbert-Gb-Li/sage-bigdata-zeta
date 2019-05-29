package com.haima.sage.bigdata.etl.reader;

import com.haima.sage.bigdata.etl.stream.SnmpTrapStream;
import org.snmp4j.MessageDispatcherImpl;
import org.snmp4j.Snmp;
import org.snmp4j.mp.MPv1;
import org.snmp4j.mp.MPv2c;
import org.snmp4j.mp.MPv3;
import org.snmp4j.security.SecurityModels;
import org.snmp4j.security.SecurityProtocols;
import org.snmp4j.security.USM;
import org.snmp4j.smi.*;
import org.snmp4j.transport.DefaultTcpTransportMapping;
import org.snmp4j.transport.DefaultUdpTransportMapping;
import org.snmp4j.util.MultiThreadedMessageDispatcher;
import org.snmp4j.util.ThreadPool;

import java.io.IOException;
import java.net.UnknownHostException;

/**
 * 本类用于监听代理进程的Trap信息
 *
 * @author zhanjia
 */
public class MultiThreadedTrapReceiver {

    private Snmp snmp = null;

    private MultiThreadedTrapReceiver() {
        // BasicConfigurator.configure();  
    }

    private void init() throws UnknownHostException, IOException {
        ThreadPool threadPool = ThreadPool.create("Trap", 2);
        MultiThreadedMessageDispatcher dispatcher = new MultiThreadedMessageDispatcher(threadPool,
                new MessageDispatcherImpl());
        Address listenAddress = GenericAddress.parse(System.getProperty(
                "snmp4j.listenAddress", "udp:127.0.0.1/1620"));
        // 对TCP与UDP协议进行处理
        if (listenAddress instanceof UdpAddress) {

            snmp = new Snmp(dispatcher, new DefaultUdpTransportMapping(
                    (UdpAddress) listenAddress));
        } else {

            snmp = new Snmp(dispatcher, new DefaultTcpTransportMapping(
                    (TcpAddress) listenAddress));
        }

        snmp.getMessageDispatcher().addMessageProcessingModel(new MPv1());
        snmp.getMessageDispatcher().addMessageProcessingModel(new MPv2c());
        snmp.getMessageDispatcher().addMessageProcessingModel(new MPv3());
        USM usm = new USM(SecurityProtocols.getInstance(), new OctetString(MPv3
                .createLocalEngineID()), 0);
        SecurityModels.getInstance().addSecurityModel(usm);
        snmp.listen();
    }


    private void run() {
        try {
            init();
            SnmpTrapStream.apply(snmp, 100);


            System.out.println("开始监听Trap信息!");
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    public static void main(String[] args) {
        MultiThreadedTrapReceiver multithreadedtrapreceiver = new MultiThreadedTrapReceiver();
        multithreadedtrapreceiver.run();
    }

} 