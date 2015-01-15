package wiki;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

public class WikiTfIdfCounterReducer
        extends Reducer<Text, Text, Text, Text>
{
    private int totalDocuments = 5094;
    private Text result = new Text();

    @Override
    public void reduce(Text word, Iterable<Text> docAtCountAtTotalsInit, Context context)
            throws IOException, InterruptedException
    {
        ArrayList<String> docAtCountAtTotals = new ArrayList<String>();
        for (Text docAtCountAtTotal : docAtCountAtTotalsInit)
        {
            docAtCountAtTotals.add(docAtCountAtTotal.toString());
        }

        int docsWithWords = docAtCountAtTotals.size();
        ArrayList<TfIdfAndDocument> results = new ArrayList<>();

        for (String docAtCountAtTotal : docAtCountAtTotals)
        {
            String[] spl = docAtCountAtTotal.split("@");
            String doc = spl[0];
            int count = Integer.valueOf(spl[1]);
            int total = Integer.valueOf(spl[2]);

            double tf = (double) count / total;
            double idf = Math.log10((double) totalDocuments / docsWithWords);
            double tfIdf = tf * idf;
            results.add(new TfIdfAndDocument(tfIdf, doc));
        }
        TfIdfAndDocument[] arr = new TfIdfAndDocument[results.size()];
        results.toArray(arr);
        Arrays.sort(arr);
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < Math.min(20, arr.length); i++)
        {
            if (i > 0)
                builder.append('\t');
            builder.append(String.format("%s:%f", arr[i].getDocument(), arr[i].getTfIdf()));
        }
        result.set(builder.toString());
        context.write(word, result);
    }
}

class TfIdfAndDocument implements Comparable<TfIdfAndDocument>
{

    private final double tfIdf;
    private final String document;

    public TfIdfAndDocument(double tfIdf, String document)
    {

        this.tfIdf = tfIdf;
        this.document = document;
    }

    public double getTfIdf()
    {
        return tfIdf;
    }

    public String getDocument()
    {
        return document;
    }

    @Override
    public int compareTo(TfIdfAndDocument o)
    {
        return tfIdf < o.tfIdf ? 1 : (tfIdf > o.tfIdf ? -1 : 0);
    }
}