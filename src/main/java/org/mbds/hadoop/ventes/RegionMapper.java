package org.mbds.hadoop.ventes;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;

import java.util.Arrays;
import java.util.StringTokenizer;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class RegionMapper extends Mapper<LongWritable, Text, Text, DoubleWritable>{
    protected void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException{

        try {
            if (key.get() == 0 && value.toString().contains("Region"))
                return;
            else {
                String data[] = value.toString().split("\n");

                for(int i = 0; i < data.length; i++){
                    String[] columns = (data[i]).split(",");
                    Text region = new Text(columns[0]);
                    Double profit = Double.parseDouble(columns[13]);
                    context.write(region, new DoubleWritable(profit));
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
