package com.haima.sage.bigdata.etl.reader;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.cloudwatch.AmazonCloudWatch;
import com.amazonaws.services.cloudwatch.AmazonCloudWatchClient;
import com.amazonaws.services.cloudwatch.model.Datapoint;
import com.amazonaws.services.cloudwatch.model.Dimension;
import com.amazonaws.services.cloudwatch.model.GetMetricStatisticsRequest;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.function.Consumer;

/**
 * Created by zhhuiyan on 15/11/30.
 */
public class AWSWatchTest {

    /*@Test
    *
    * FIXME when have an aws env
    * */
    public void testDataPoint(){
        AWSCredentials credentials  = new BasicAWSCredentials("AKIAP73ALCIUYVKG6K5Q", "Yjyi1eFGPSPYWGPdyUdBZqO0mXgE0IfB/ot65Dwc");
        AmazonCloudWatch cloudWatch = new AmazonCloudWatchClient(credentials);
        Region cn_north1 = Region.getRegion(Regions.CN_NORTH_1);
        cloudWatch.setRegion(cn_north1);
        Calendar calendar = Calendar.getInstance();
        calendar.set(Calendar.HOUR_OF_DAY, 0);
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.SECOND, 0);
        calendar.set(Calendar.YEAR, 2015);
        calendar.set(Calendar.MONTH, 11);
        calendar.set(Calendar.DAY_OF_MONTH, 23);

        Date start = calendar.getTime();
        calendar.set(Calendar.DAY_OF_MONTH, 30);

        Date end = calendar.getTime();
        GetMetricStatisticsRequest request = new GetMetricStatisticsRequest();

        request.setStartTime(start);



        request.setEndTime(end);
        request.setNamespace("AWS/S3");
        request.setMetricName("BucketSizeBytes");

        /*
        * metric:{Namespace: AWS/S3,MetricName: NumberOfObjects,Dimensions: [{Name: BucketName,Value: hslog}, {Name: StorageType,Value: AllStorageTypes}]}
        * */
        List<Dimension> dimensions =new ArrayList<>();
        dimensions.add(new Dimension().withName("BucketName").withValue("hslog"));
        dimensions.add(new Dimension().withName("StorageType").withValue("StandardStorage"));
        request.setDimensions(dimensions);
        request.setPeriod(86400);
        request.setUnit("Bytes");
        List<String> statistics =new ArrayList<>();
        statistics.add("Average");

        request.setStatistics(statistics);
        cloudWatch.getMetricStatistics(request).getDatapoints().forEach(new Consumer<Datapoint>() {
            @Override
            public void accept(Datapoint datapoint) {
                System.out.println("datapoint = " + datapoint);
            }
        });

    }
}
