package wiki;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class WikiWordsDocsCounterMapper
        extends Mapper<Text, Text, Text, IntWritable>
{
    private Text nkey = new Text();
    private final static IntWritable one = new IntWritable(1);
    private static String pattern = "\\P{InCyrillic}+";
    private String[] skipList = new String[]{
            pattern,
            "года",
            "что",
            "году",
            "категория",
            "для",
            "как",
            "россии",
            "его",
            "также",
            "был",
            "путин",
            "или",
            "при",
            "путина",
            "время",
            "новой",
            "было",
            "это",
            "были",
            "после"
    };

    @Override
    public void map(Text docId, Text text, Context context)
            throws IOException, InterruptedException
    {
        String txt = new String(text.getBytes(), "UTF-8").toLowerCase();
        for (String pattern : skipList)
            txt = txt.replaceAll(pattern, " ");
        StringTokenizer tokenizer = new StringTokenizer(txt);
        while (tokenizer.hasMoreElements())
        {
            String current = tokenizer.nextToken();
            if (current.length() > 2)
            {
                nkey.set(current + "@" + docId);
                context.write(nkey, one);
            }
        }
    }
}

