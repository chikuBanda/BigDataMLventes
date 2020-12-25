package com.chiku.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Main {
    public static void main(String[] args) throws Exception{
        Configuration configuration = new Configuration();
        String typeAnalyse = args[2];

        if(typeAnalyse.compareToIgnoreCase("profitParRegion") == 0){
            Job job = Job.getInstance(configuration, "Profit par region");

            job.setJarByClass(Main.class);
            job.setMapperClass(RegionMapper.class);
            job.setReducerClass(RegionReducer.class);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(DoubleWritable.class);

            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));

            if(job.waitForCompletion(true))
                System.exit(0);
        }
        else if(typeAnalyse.compareToIgnoreCase("profitParPays") == 0){
            Job job = Job.getInstance(configuration, "Profit par pays");

            job.setJarByClass(Main.class);
            job.setMapperClass(CountryMapper.class);
            job.setReducerClass(CountryReducer.class);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(DoubleWritable.class);

            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));

            if(job.waitForCompletion(true))
                System.exit(0);
        }
        else if(typeAnalyse.compareToIgnoreCase("profitParTypeItem") == 0){
            Job job = Job.getInstance(configuration, "Profit par type item");

            job.setJarByClass(Main.class);
            job.setMapperClass(ItemMapper.class);
            job.setReducerClass(ItemReducer.class);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(CustomDoubleWritable.class);

            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));

            if(job.waitForCompletion(true))
                System.exit(0);
        }
        else{
            System.out.println("arguments non valides!!");
        }

        System.exit(-1);
    }
}