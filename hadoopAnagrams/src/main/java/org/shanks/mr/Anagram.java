package org.shanks.mr;

import java.io.IOException;
import java.util.Arrays;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Anagram {
    public static class AnagramMapper
            extends Mapper<Object, Text, Text, Text>{
        private Text word = new Text();
        private Text sortedText = new Text();
        private Text orginalText = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                char[] wordChars = word.toString().toCharArray();
                Arrays.sort(wordChars);
                String sortedWord = new String(wordChars);
                sortedText.set(sortedWord);
                orginalText.set(word);
                context.write(sortedText, orginalText);
            }
        }
    }

    public static class AnagramReducer extends Reducer<Text,Text,Text,Text> {
        private Text result = new Text();
        public void reduce(Text key, Iterable<Text> values, Context context
        ) throws IOException, InterruptedException {
            String anagramlist = "";
            int length = 0;
            for(Text val : values){
                anagramlist+= "," + val.toString();
                length++;
            }
            if(length>1){
                System.out.println(anagramlist);
                result.set(anagramlist);
                context.write(key,result);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "anagram");
        job.setJarByClass(Anagram.class);
        job.setMapperClass(AnagramMapper.class);
        job.setReducerClass(AnagramReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }
}