package wiki;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WikiWordsAggregatorReducer
        extends Reducer<Text, IntWritable, Text, LongWritable>
{
    private LongWritable nvalue = new LongWritable();

    public void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException
    {
        long sum = 0;
        for (IntWritable val : values)
        {
            sum += val.get();
        }
        nvalue.set(sum);
        context.write(key, nvalue);
    }
}

