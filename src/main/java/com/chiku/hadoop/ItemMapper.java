package com.chiku.hadoop;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class ItemMapper extends Mapper<LongWritable, Text, Text, CustomDoubleWritable> {
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException{

        try {
            if (key.get() == 0 && value.toString().contains("Region"))
                return;
            else {
                String data[] = value.toString().split("\n");

                for(int i = 0; i < data.length; i++){
                    String[] columns = (data[i]).split(",");
                    Text itemType = new Text(columns[2]);
                    Double profit = Double.parseDouble(columns[13]);
                    String channel = columns[3];
                    CustomDoubleWritable customDoubleWritable;

                    if(channel.compareToIgnoreCase("online") == 0){
                        customDoubleWritable = new CustomDoubleWritable(profit, 0);
                        context.write(itemType, customDoubleWritable);
                    }
                    else{
                        customDoubleWritable = new CustomDoubleWritable(0, profit);
                        context.write(itemType, customDoubleWritable);
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
