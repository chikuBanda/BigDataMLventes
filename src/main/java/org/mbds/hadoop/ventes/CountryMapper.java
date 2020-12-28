package org.mbds.hadoop.ventes;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class CountryMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException{

        try {
            if (key.get() == 0 && value.toString().contains("Region"))
                return;
            else {
                String data[] = value.toString().split("\n");

                for(int i = 0; i < data.length; i++){
                    String[] columns = (data[i]).split(",");
                    Text country = new Text(columns[1]);
                    Double profit = Double.parseDouble(columns[13]);
                    context.write(country, new DoubleWritable(profit));
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
