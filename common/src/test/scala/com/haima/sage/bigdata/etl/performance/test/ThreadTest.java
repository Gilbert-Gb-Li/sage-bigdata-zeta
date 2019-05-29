package com.haima.sage.bigdata.etl.performance.test;


import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;

// 实现Callable接口来实现线程
class ThreadByCallable implements Callable<Integer> {
    Callable c;

    public ThreadByCallable(){}

    public ThreadByCallable(Callable c) {
        this.c = c;
    }

    @Override
    public Integer call() {
        System.out.println("当前线程名称是：" + Thread.currentThread().getName());

        int i = 0;
        for (; i < 5; i++) {
            try {
                Thread.sleep(2000);
                System.out.println("循环变量i的值：" + i);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        // call()方法有返回值
        return i;
    }

    public static void main(String[] args) {
        new ThreadByCallable().go();
        Object o;
    }

    public void go(){
        ThreadByCallable rt = new ThreadByCallable(this);
        // 使用FutureTask来包装Callable对象
        FutureTask<Integer> task = new FutureTask<Integer>(rt);
        new Thread(task, "有返回值的线程").start();
        try {
            // 获取线程返回值
            System.out.println("子线程的返回值：" + task.get(2, TimeUnit.SECONDS));
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
}
