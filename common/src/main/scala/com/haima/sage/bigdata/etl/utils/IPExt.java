/**
 * @date 2015年12月14日 上午10:31:54
 */
package com.haima.sage.bigdata.etl.utils;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @date 2015年12月14日 上午10:31:54
 */

public class IPExt  {

    private static boolean enableFileWatch = false;

    private static int offset;
    private static int[] index = new int[65536];
    private static ByteBuffer dataBuffer;
    private static ByteBuffer indexBuffer;
    private static Long lastModifyTime = 0L;
    private static File ipFile;
    private static ReentrantLock lock = new ReentrantLock();
//    public static Map<String, String> provinceCityMap = new HashMap<String, String>();

//    static {
//        provinceCityMap.put("北京", "BEIJING");
//        provinceCityMap.put("上海", "SHANGHAI");
//        provinceCityMap.put("天津", "TIANJIN");
//        provinceCityMap.put("重庆", "CHONGQIN");
//        provinceCityMap.put("内蒙古", "HUHEHAOTE");//呼和浩特
//        provinceCityMap.put("宁夏", "YINCHUAN");//银川
//        provinceCityMap.put("新疆", "WULUMUQI");//乌鲁木齐
//        provinceCityMap.put("西藏", "LASA");//拉萨
//        provinceCityMap.put("广西", "NANNING");//南宁
//        provinceCityMap.put("香港", "XIANGGANG");
//        provinceCityMap.put("澳门", "AOMEN");
//        provinceCityMap.put("黑龙江", "HAERBIN");//哈尔滨
//        provinceCityMap.put("吉林", "CHANGCHUN");//长春
//        provinceCityMap.put("辽宁", "SHENYANG");//沈阳
//        provinceCityMap.put("江西", "NANCHANG");//南昌
//        provinceCityMap.put("江苏", "NANJING");//南京
//        provinceCityMap.put("山东", "JINAN");//济南
//        provinceCityMap.put("安徽", "HEFEI");//合肥
//        provinceCityMap.put("河北", "SHIJIAZHUANG");//石家庄
//        provinceCityMap.put("河南", "ZHENGZHOU");//郑州
//        provinceCityMap.put("湖北", "WUHAN");//武汉
//        provinceCityMap.put("湖南", "CHANGSHA");//长沙
//        provinceCityMap.put("陕西", "XIAN");//西安
//        provinceCityMap.put("山西", "TAIYUAN");//太原
//        provinceCityMap.put("四川", "CHENGDU");//成都
//        provinceCityMap.put("青海", "XINING");//西宁
//        provinceCityMap.put("海南", "HAIKOU");//海口
//        provinceCityMap.put("广东", "GUANGZHOU");//广州
//        provinceCityMap.put("贵州", "GUIYANG");//贵阳
//        provinceCityMap.put("浙江", "HANGZHOU");//杭州
//        provinceCityMap.put("福建", "FUZHOU");//福州
//        provinceCityMap.put("台湾", "TAIPEI");//台北
//        provinceCityMap.put("甘肃", "LANZHOU");//兰州
//        provinceCityMap.put("云南", "KUNMING");//昆明
//        provinceCityMap.put("局域网", "BAODING");//默认保定
//
//    }

    public static void main(String[] args) {
        IPExt.load("ip_location.datx");
        System.out.println(Arrays.toString(IPExt.find("119.142.168.233")));
        System.out.println(Arrays.toString(IPExt.find("112.124.219.82")));
        System.out.println(Arrays.toString(IPExt.find("215.117.112.84")));

       /* ip2long("192.168.0.0");*/
    }

    private static String ipMapperFilePath(String fileName) {
        String filePath = fileName;
        if (!fileName.startsWith("/")) {
            URL url = IPExt.class.getClassLoader().getResource(fileName);
            filePath = url.getPath();
        }
        if (filePath.startsWith("file")) {
            filePath = filePath.substring(5);
        }
        filePath = filePath.replace("/", File.separator);
        return filePath;
    }

    public static void load(String filename) {
        String filePath = ipMapperFilePath(filename);
        ipFile = new File(filePath);
        load();
        if (enableFileWatch) {
            watch();
        }
    }

    public static void load(String filename, boolean strict) throws Exception {
        ipFile = new File(filename);
        if (strict) {
            int contentLength = Long.valueOf(ipFile.length()).intValue();
            if (contentLength < 512 * 1024) {
                throw new Exception("ip data file error.");
            }
        }
        load();
        if (enableFileWatch) {
            watch();
        }
    }

    public static String[] find(String ip) {
        String[] ips = ip.split("\\.");
        int prefix_value = (Integer.valueOf(ips[0]) * 256 + Integer.valueOf(ips[1]));
        long ip2long_value = ip2long(ip);
        int start = index[prefix_value];
        int max_comp_len = offset - 262144 - 4;
        long tmpInt;
        long index_offset = -1;
        int index_length = -1;
        byte b = 0;
        for (start = start * 9 + 262144; start < max_comp_len; start += 9) {
            tmpInt = int2long(indexBuffer.getInt(start));
            if (tmpInt >= ip2long_value) {
                index_offset = bytesToLong(b, indexBuffer.get(start + 6), indexBuffer.get(start + 5), indexBuffer.get(start + 4));
                index_length = (0xFF & indexBuffer.get(start + 7) << 8) + (0xFF & indexBuffer.get(start + 8));
                break;
            }
        }

        byte[] areaBytes;

        lock.lock();
        try {
            dataBuffer.position(offset + (int) index_offset - 262144);
            areaBytes = new byte[index_length];
            dataBuffer.get(areaBytes, 0, index_length);
        } finally {
            lock.unlock();
        }

        return new String(areaBytes, Charset.forName("UTF-8")).split("\t", -1);
    }

    private static void watch() {
        Executors.newScheduledThreadPool(1).scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                long time = ipFile.lastModified();
                if (time > lastModifyTime) {
                    load();
                }
            }
        }, 1000L, 5000L, TimeUnit.MILLISECONDS);
    }

    private static void load() {
        if(ipFile!=null && ipFile.exists() && ipFile.isFile()){
            lastModifyTime = ipFile.lastModified();
            lock.lock();
            try {
                dataBuffer = ByteBuffer.wrap(getBytesByFile(ipFile));
                dataBuffer.position(0);
                offset = dataBuffer.getInt(); // indexLength
                byte[] indexBytes = new byte[offset];
                dataBuffer.get(indexBytes, 0, offset - 4);
                indexBuffer = ByteBuffer.wrap(indexBytes);
                indexBuffer.order(ByteOrder.LITTLE_ENDIAN);

                for (int i = 0; i < 256; i++) {
                    for (int j = 0; j < 256; j++) {
                        index[i * 256 + j] = indexBuffer.getInt();
                    }
                }
                indexBuffer.order(ByteOrder.BIG_ENDIAN);
            } finally {
                lock.unlock();
            }
        }


    }

    private static byte[] getBytesByFile(File file) {
        if(file.exists()){
            FileInputStream fin = null;
            byte[] bs = new byte[new Long(file.length()).intValue()];
            try {
                fin = new FileInputStream(file);
                int readBytesLength = 0;
                int i;
                while ((i = fin.available()) > 0) {
                    fin.read(bs, readBytesLength, i);
                    readBytesLength += i;
                }
            } catch (IOException ioe) {
                ioe.printStackTrace();
            } finally {
                try {
                    if (fin != null) {
                        fin.close();
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

            return bs;
        }else{
            return null;
        }


    }

    private static long bytesToLong(byte a, byte b, byte c, byte d) {
        return int2long((((a & 0xff) << 24) | ((b & 0xff) << 16) | ((c & 0xff) << 8) | (d & 0xff)));
    }

    private static int str2Ip(String ip) {
        String[] ss = ip.split("\\.");
        int a, b, c, d;
        a = Integer.parseInt(ss[0]);
        b = Integer.parseInt(ss[1]);
        c = Integer.parseInt(ss[2]);
        d = Integer.parseInt(ss[3]);
        return (a << 24) | (b << 16) | (c << 8) | d;
    }

    private static long ip2long(String ip) {
        return int2long(str2Ip(ip));
    }

    private static long int2long(int i) {
        long l = i & 0x7fffffffL;
        if (i < 0) {
            l |= 0x080000000L;
        }
        return l;
    }


}