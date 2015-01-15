package wiki;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WikiDocsCounterMapper
        extends Mapper<Text, Text, Text, Text>
{
    private Text nkey = new Text();
    private Text nvalue = new Text();

    @Override
    public void map(Text wordAtDoc, Text count, Context context)
            throws IOException, InterruptedException
    {
        String[] spl = wordAtDoc.toString().split("@");
        String word = spl[0];
        String doc = spl[1];
        nkey.set(doc);
        nvalue.set(word + "@" + count.toString());
        context.write(nkey, nvalue);
    }
}

