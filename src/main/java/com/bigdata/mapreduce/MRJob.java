package com.bigdata.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class MRJob {
    public static class Map extends Mapper<LongWritable, Text, Text, IntermediateValue> {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().replace("\"", "");
            if (line.contains("transaction")) {
                return;
            }
            String[] data = line.split(",");
            String outputKey = data[2] + "," + data[6].split(" ")[0];
            IntermediateValue outputValue = new IntermediateValue();
            outputValue.setPrice(Double.parseDouble(data[5]));
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            Date date = null;
            try {
                date = sdf.parse(data[3]);
            } catch (ParseException e) {
                e.printStackTrace();
            }
            if (date != null) {
                outputValue.setEpochTime(date.getTime());
            }
            context.write(new Text(outputKey), outputValue);
        }
    }

    public static class PricePartitioner extends Partitioner<Text, IntermediateValue> {
        @Override
        public int getPartition(Text text, IntermediateValue intermediateValue, int numPartitions) {
            int i = 0;
            Double price = intermediateValue.getPrice();
            if (price > 5000d && price <= 10000d) {
                i = 1;
            } else if (price > 10000 && price <= 20000) {
                i = 2;
            } else if (price > 20000) {
                i = 3;
            }
            return (i % numPartitions);
        }
    }

    public static class Reduce extends Reducer<Text, IntermediateValue, Text, FinalValue> {
        @Override
        public void reduce(Text key, Iterable<IntermediateValue> values, Context context) throws IOException, InterruptedException {
            FinalValue fv = new FinalValue();
            long minEpoch = Long.MAX_VALUE;
            long maxEpoch = 0;
            for (IntermediateValue val : values) {
                fv.setTotalTransactions(fv.getTotalTransactions() + 1);
                fv.setAveragePrice(fv.getAveragePrice() + (val.getPrice() - fv.getAveragePrice()) / fv.getTotalTransactions());
                if(val.getEpochTime() != 0){
                    minEpoch = Math.min(minEpoch, val.getEpochTime());
                    maxEpoch = Math.max(maxEpoch, val.getEpochTime());
                }
            }
            if(maxEpoch != Long.MAX_VALUE){
                fv.setEpochDiff(maxEpoch - minEpoch);
            }
            context.write(key, fv);
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if(otherArgs.length != 2){
            System.out.println("Usage: <filename> <input_file> <output_directory>");
            System.exit(1);
        }
        Job job = Job.getInstance(conf, "Merchant Data Analysis");
        job.setJarByClass(MRJob.class);
        job.setMapperClass(Map.class);
        job.setPartitionerClass(PricePartitioner.class);
        job.setReducerClass(Reduce.class);
        int tasks = 4;
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntermediateValue.class);
        job.setNumReduceTasks(tasks);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FinalValue.class);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        if(job.waitForCompletion(true)){
            System.exit(0);
        }
        System.exit(0);
    }
}
