package edu.uprm.cse.bigdata.p1exam1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.lang.StringBuilder;
/**
 * Created by broecat on 03-30-17.
 */
public class TaskOneReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

       StringBuilder sb = new StringBuilder();

        for(Text lw: values)
        {
            sb.append(", "+lw.toString());
        }
       
        context.write(key, new Text(sb.toString()));
    }
}