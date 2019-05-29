package com.haima.sage.bigdata.etl.performance.test;


import sun.misc.Unsafe;
import sun.nio.ch.DirectBuffer;

import java.io.IOException;
import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousServerSocketChannel;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.CompletionHandler;

import java.util.concurrent.BlockingQueue;

public class JavaTest {

    public static void main(String[] args) {

        String car_name ="哈弗 H6 Coupe 2018款 1.5T 自动 蓝标超豪型前驱";
        String name = car_name.split("\\s")[0];
        char[] names = name.toCharArray();
        int i = 0;
        while (i<names.length) {
            if (names[i]>='0' && names[i]<='9'
                    || names[i]>='A' && names[i]<='Z'
                    ||  names[i]>='a' && names[i]<='z') {
                break;
            }
            i++;
        }
        System.out.println("i: " + i);
        System.out.println(car_name.indexOf(' '));
//        String brand = car_name.substring(0, i).trim().replace("汽车", "");
//        String series= car_name.substring(i, car_name.indexOf("款")-4).trim();
//        System.out.println("brand: " + brand + ", series: " + series);



    }

    public void run() throws InterruptedException {

        Thread t = new Thread("Thread one "){
            @Override
            public void run() {
                try{
                    for (int i=0; i<100; i++){
                        Thread.sleep(1000);
                        System.out.println(Thread.currentThread().getName() + i);
                        /** 自身设置中断
                        if (i == 5) Thread.currentThread().interrupt();
                         */
                        /**
                         * 通过逻辑判断是否中断来退出，
                         * return 可以退出当前线程，break只会退出当前循环；
                         */
                        if (currentThread().isInterrupted()) return;
                    }
                    System.out.println(Thread.currentThread().getName() + "over");
                } catch (InterruptedException e){
                    /** 自身或其他线程设置的中断会被此catch捕获*/
                    System.out.println(Thread.currentThread().getName() + "interrupted!");
                }

            }
        };
        t.start();
        Thread.sleep(3000);
        t.interrupt();

        new AnnymeTest() {
            @Override
            public void aShow() {
                System.out.println(a);
            }

            @Override
            public void apulse() {
                a++;
                System.out.println(a);
            }
        }.apulse();
        long time = System.currentTimeMillis();
        System.out.println(time);
    }




}

class Person {
    private String name;
    private int age;
    private Child c;
    public Person(){}

    public Person(String name){
        this.name = name;
        this.c = new Child();
    }

    public Person(String name, int age){
        this(name);
        this.age = age;
    }

    public void display(){
        System.out.println(mkString());
    }

    public String mkString(){
        return "name: " + c.getName() + ", age: " + age;
    }

    class Child {
        private String name;

        public String getName() {
            return name;
        }

        public void setName() {
            name = Person.this.name;
        }
    }


}

class ChannelHandler implements CompletionHandler<Integer, String> {

    @Override
    public void completed(Integer result, String attachment) {
    }

    @Override
    public void failed(Throwable exc, String attachment) {

    }


}

abstract class AnnymeTest{
    int a = 0;
    public abstract void aShow();
    public abstract void apulse();
}

class UnsafeTest{
    public static void main(String[] args) throws Exception{
        Field f = Unsafe.class.getDeclaredField("theUnsafe");
        f.setAccessible(true);
        Unsafe unsafe = (Unsafe) f.get(null);
        long start = unsafe.allocateMemory(32);
        System.out.println(start);
        unsafe.setMemory(start,32, (byte)0);
        unsafe.putByte(start, (byte) 'a');
        unsafe.putByte(start + 1, (byte) 'b');
        byte b = unsafe.getByte(start + 1);
        System.out.println((char)b);
        unsafe.freeMemory(start);


    }



    public static void clean(final ByteBuffer buffer,final Unsafe unsafe){
        ByteBuffer bb = ByteBuffer.allocateDirect(1024);
        bb.get();
        if (buffer.isDirect()){
            ((DirectBuffer) buffer).cleaner().clean();
        }
    }
}

