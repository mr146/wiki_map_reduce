package wiki;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WikiDocumentsCounterMapper
        extends Mapper<Text, Text, IntWritable, IntWritable>
{
    private final static IntWritable one = new IntWritable(1);

    @Override
    public void map(Text key, Text value, Context context)
            throws IOException, InterruptedException
    {
        context.write(one, one);
    }
}
