package com.haima.sage.bigdata.etl.quartz

import java.util.Date

import org.quartz.CronScheduleBuilder.cronSchedule
import org.quartz.JobBuilder.newJob
import org.quartz.TriggerBuilder.newTrigger
import org.quartz.impl.StdSchedulerFactory
import org.quartz.{Job, JobExecutionContext, JobExecutionException, SchedulerException}

/**
  * Created by zhhuiyan on 2017/4/5.
  * Email: weiping_he@
  */
object CrontabrTest {
  private[quartz] val sf = new StdSchedulerFactory

  @throws[SchedulerException]
  def main(args: Array[String]): Unit = {
    val sched = sf.getScheduler
    val job = newJob(classOf[QuartzTest]).withIdentity("job1", "group1").build
    val trigger = newTrigger.withIdentity("trigger1", "group1").withSchedule(cronSchedule("0/10 * * * * ?")).build
    sched.scheduleJob(job, trigger)
    sched.start()
  }

  class QuartzTest extends Job {
    @throws[JobExecutionException]
    override def execute(context: JobExecutionContext): Unit = {
      val jobKey = context.getJobDetail.getKey
      System.out.println("SimpleJob says: " + jobKey + " executing at " + new Date)
    }
  }
}
