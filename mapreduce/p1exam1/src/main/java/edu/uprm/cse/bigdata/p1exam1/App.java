package edu.uprm.cse.bigdata.p1exam1;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {
      try {
            Job job = new Job();
            job.setJarByClass(edu.uprm.cse.bigdata.p1exam1.App.class);
            job.setJobName("Count Occurrences of Specific Words");

            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));

            job.setMapperClass(edu.uprm.cse.bigdata.p1exam1.TaskMapper.class);
            job.setReducerClass(edu.uprm.cse.bigdata.p1exam1.TaskReducer.class);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            job.waitForCompletion(true);
            System.exit(0);
        }
        catch(Exception ex)
        {
            System.out.println(ex.getMessage());
            System.exit(0);
        }
    }
}
