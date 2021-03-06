package wiki;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class WikiWordsAggregatorMapper
        extends Mapper<Text, Text, Text, IntWritable>
{

    private final static IntWritable one = new IntWritable(1);
    private static String pattern = "\\P{InCyrillic}+";
    private Text nkey = new Text();


    @Override
    public void map(Text docId, Text text, Context context)
            throws IOException, InterruptedException
    {
        String txt = new String(text.getBytes(), "UTF-8").toLowerCase();

        txt = txt.replaceAll(pattern, " ");
        StringTokenizer tokenizer = new StringTokenizer(txt);
        while (tokenizer.hasMoreElements())
        {
            String current = tokenizer.nextToken();
            if (current.length() > 2)
            {
                nkey.set(current);
                context.write(nkey, one);
            }
        }
    }
}

