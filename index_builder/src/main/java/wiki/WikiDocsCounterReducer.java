package wiki;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;

public class WikiDocsCounterReducer
        extends Reducer<Text, Text, Text, Text>
{
    private Text nkey = new Text();
    private Text nvalue = new Text();

    @Override
    public void reduce(Text doc, Iterable<Text> wordAtCounts, Context context)
            throws IOException, InterruptedException
    {
        int cnt = 0;
        String docS = doc.toString();
        HashMap<String, String> counter = new HashMap<String, String>();
        for (Text wordAtCount : wordAtCounts)
        {
            String[] spl = wordAtCount.toString().split("@");
            String word = spl[0];
            String count = spl[1];
            cnt += Integer.valueOf(count);
            counter.put(word, count);
        }
        for (String word : counter.keySet())
        {
            nkey.set(word + "@" + docS);
            nvalue.set(counter.get(word) + "@" + cnt);
            context.write(nkey, nvalue);
        }
    }
}

