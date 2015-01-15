package wiki;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WikiDocumentsCounterReducer
        extends Reducer<IntWritable, IntWritable, LongWritable, Text>
{
    private LongWritable nkey = new LongWritable();
    private static final Text emptyText = new Text();

    public void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException
    {
        long sum = 0;
        for (IntWritable val : values)
        {
            sum += val.get();
        }
        nkey.set(sum);
        context.write(nkey, emptyText);
    }
}
