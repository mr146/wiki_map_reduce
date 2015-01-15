package wiki;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WikiWordsCounterReducer
        extends Reducer<LongWritable, Text, Text, Text>
{
    private static final Text emptyText = new Text();
    private int counter = 0;
    public void reduce(LongWritable key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException
    {
        for (Text val : values)
        {
            if(counter < 20)
            {
                context.write(val, emptyText);
                counter++;
            }
            else
                break;
        }
    }
}
