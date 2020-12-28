package org.shanks;

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Sales {
    public static class SalesMapper
            extends Mapper<Text, Text, Text, customReturnValueWritable>{
        private Text currentLine = new Text();
        private String region;
        private String country;
        private String itemType;
        private String totalProfit;
        private IntWritable onlineQuantity = new IntWritable();
        private IntWritable offlineQuantity = new IntWritable();

        public void map(Text key, Text value, Context context
        ) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            String taskType = conf.get("taskType");
            StringTokenizer itr = new StringTokenizer(key.toString(), "\n");
            switch (taskType){
                case "region":
                    while (itr.hasMoreTokens()) {
                        currentLine.set(itr.nextToken());
                        region = currentLine.toString().split(",")[0];
                        if(!region.equals("Region")){
                            totalProfit = currentLine.toString().split(",")[13];
                            customReturnValueWritable ivw = new customReturnValueWritable();
                            ivw.setTotalProfit(new DoubleWritable(Double.parseDouble(totalProfit)));
                            context.write(new Text(region), ivw);
                        }
                    }
                     break;
                case "country":
                    while (itr.hasMoreTokens()) {
                        currentLine.set(itr.nextToken());
                        country = currentLine.toString().split(",")[1];
                        if(!country.equals("Country")){
                            totalProfit = currentLine.toString().split(",")[13];
                            customReturnValueWritable ivw = new customReturnValueWritable();
                            ivw.setTotalProfit(new DoubleWritable(Double.parseDouble(totalProfit)));
                            context.write(new Text(country), ivw);
                        }
                    }
                    break;
                case "item-type":
                    while (itr.hasMoreTokens()) {
                        customReturnValueWritable returnValue = new customReturnValueWritable();
                        DoubleWritable itemTotal;
                        currentLine.set(itr.nextToken());
                        itemType = currentLine.toString().split(",")[2];
                        if(!itemType.equals("Item Type")){
                            totalProfit = currentLine.toString().split(",")[13];
                            if(currentLine.toString().split(",")[3].equals("Online")){
                                onlineQuantity = new IntWritable(Integer.parseInt(currentLine.toString().split(",")[8]));
                                offlineQuantity = new IntWritable(0);
                            }
                            else {
                                onlineQuantity = new IntWritable(0);
                                offlineQuantity = new IntWritable(Integer.parseInt(currentLine.toString().split(",")[8]));
                            }
                            itemTotal = new DoubleWritable(Double.parseDouble(totalProfit));
                            returnValue
                                    .setQuantityOnline(onlineQuantity);
                            returnValue
                                    .setQuantityOffline(offlineQuantity);
                            returnValue
                                    .setTotalProfit(itemTotal);
                            context.write(new Text(itemType), returnValue);
                        }
                    }
                    break;
            }
        }
    }

    public static class SalesReducer extends Reducer<Text, customReturnValueWritable,Text,Text> {
        private double profitSum = 0;
        private double offlineQuantity = 0;
        private double onlineQuantity = 0;
        private String formatValue;
        public void reduce(Text key, Iterable<customReturnValueWritable> values, Context context
        ) throws IOException, InterruptedException {
            String taskType = context.getConfiguration().get("taskType");
            if(!taskType.equals("item-type")){
                for(customReturnValueWritable value: values){
                    profitSum += value.getTotalProfit().get();
                }
                context.write(key, new Text(String.valueOf(profitSum)));
            }
            else {
                for(customReturnValueWritable value: values){
                    profitSum += value.getTotalProfit().get();
                    offlineQuantity += value.getQuantityOffline().get();
                    onlineQuantity += value.getQuantityOnline().get();
                }
                formatValue =
                        "| online quantity : " + onlineQuantity + " | offline : " + offlineQuantity + " | total profit: " + profitSum;
                context.write(key, new Text(formatValue));
            }
            profitSum = 0;
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("taskType", args[2]);
        Job job = Job.getInstance(conf, "Sales");
        job.setJarByClass(Sales.class);
        job.setMapperClass(SalesMapper.class);
        job.setReducerClass(SalesReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(customReturnValueWritable.class);

        //job.setInputFormatClass(KeyValueTextInputFormat.class);
        job.setInputFormatClass(KeyValueTextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }
}