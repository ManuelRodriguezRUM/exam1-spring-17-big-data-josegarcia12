package edu.uprm.cse.bigdata.p1exam1;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import twitter4j.Status;
import twitter4j.TwitterException;
import twitter4j.TwitterObjectFactory;

import java.io.IOException;

/**
 * Created by broecat on 03-30-17.
 */
public class TaskMapper extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String rawTweet = value.toString();
        String[] words = {"MAGA","DICTATOR","IMPEACH","DRAIN","SWAMP","CHANGE"};
        try {
            Status status = TwitterObjectFactory.createStatus(rawTweet);
            String tweet = status.getText().toUpperCase();

            for(String s: words)
            {
                if(tweet.contains(s))
                {
                    context.write(new Text(s),new Text(status.getId()+""));
                }
            }
        }
        catch(TwitterException e){

        }

    }
}