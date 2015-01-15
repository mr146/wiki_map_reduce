package wiki;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WikiWordsCounterMapper
        extends Mapper<Text, Text, LongWritable, Text>
{
    private LongWritable nkey = new LongWritable();

    @Override
    public void map(Text word, Text count, Context context)
            throws IOException, InterruptedException
    {
        long countVal = Long.parseLong(count.toString());
        nkey.set(-countVal);
        context.write(nkey, word);
    }
}
