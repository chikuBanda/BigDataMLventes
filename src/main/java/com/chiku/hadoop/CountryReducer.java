package com.chiku.hadoop;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class CountryReducer extends Reducer<Text, DoubleWritable, Text, Text> {
    public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
            throws IOException, InterruptedException {

        Iterator<DoubleWritable> i = values.iterator();
        double profitTotale = 0;

        while(i.hasNext()){
            profitTotale += i.next().get();
        }

        context.write(key, new Text("\tProfit totale=" + String.format("%.2f", profitTotale)));
    }
}
