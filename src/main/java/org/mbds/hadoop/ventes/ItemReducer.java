package org.mbds.hadoop.ventes;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class ItemReducer extends Reducer<Text, CustomDoubleWritable, Text, Text> {
    public void reduce(Text key, Iterable<CustomDoubleWritable> values, Context context)
            throws IOException, InterruptedException {

        Iterator<CustomDoubleWritable> i = values.iterator();
        double profitTotaleOnline = 0;
        double profitTotaleOffline = 0;

        while(i.hasNext()){
            CustomDoubleWritable iter = i.next();
            profitTotaleOnline += iter.getDouble1().get();
            profitTotaleOffline += iter.getDouble2().get();
        }

        context.write(
                key,
                new Text(
                        "\tProfit totale (online)="
                                + String.format("%.2f", profitTotaleOnline)
                                + "\t\t\tProfit totale (offline)="
                                + String.format("%.2f", profitTotaleOffline)
                )
        );
    }
}
