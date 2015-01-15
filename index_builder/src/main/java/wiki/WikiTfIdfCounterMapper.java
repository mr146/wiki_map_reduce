package wiki;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WikiTfIdfCounterMapper
        extends Mapper<Text, Text, Text, Text>
{
    private Text nkey = new Text();
    private Text nvalue = new Text();

    @Override
    public void map(Text wordAtDoc, Text countAtTotal, Context context)
            throws IOException, InterruptedException
    {
        String[] spl = wordAtDoc.toString().split("@");
        String word = spl[0];
        String doc = spl[1];
        nkey.set(word);
        nvalue.set(doc + "@" + countAtTotal.toString());
        context.write(nkey, nvalue);
    }
}
