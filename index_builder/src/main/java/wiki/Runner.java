package wiki;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;


public class Runner extends Configured implements Tool
{

    private Job buildWordsDocsCounterJob(String input, String output) throws IOException
    {
        Job job = Job.getInstance();
        job.setJobName("wordsdocsaggregator");
        job.setJarByClass(getClass());
        job.setNumReduceTasks(1);

        job.setInputFormatClass(KeyValueTextInputFormat.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(WikiWordsDocsCounterMapper.class);
        job.setReducerClass(WikiWordsDocsCounterReducer.class);

        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));
        return job;
    }

    private Job buildDocsCounterJob(String input, String output) throws IOException
    {
        Job job = Job.getInstance();
        job.setJobName("docscounter");
        job.setJarByClass(getClass());
        job.setNumReduceTasks(1);

        job.setInputFormatClass(KeyValueTextInputFormat.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(WikiDocsCounterMapper.class);
        job.setReducerClass(WikiDocsCounterReducer.class);

        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));
        return job;
    }

    private Job buildTfIdfCounterJob(String input, String output) throws IOException
    {
        Job job = Job.getInstance();
        job.setJobName("tfidfcounter");
        job.setJarByClass(getClass());
        job.setNumReduceTasks(1);

        job.setInputFormatClass(KeyValueTextInputFormat.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(WikiTfIdfCounterMapper.class);
        job.setReducerClass(WikiTfIdfCounterReducer.class);

        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));
        return job;
    }

    public int run(String[] args) throws Exception
    {
        if (args.length != 2)
        {
            System.err.printf("Usage: %s [generic options] <input> <output>\n",
                    getClass().getSimpleName());
            ToolRunner.printGenericCommandUsage(System.err);
            return -1;
        }

        String wordsDocsOutput = "wiki/tfidf/wordsdocs";
        String docsOutput = "wiki/tfidf/docs";
        String tfIdfOutput = "wiki/index";

        Job aggregator = buildWordsDocsCounterJob(args[0], wordsDocsOutput);
        Job counter = buildDocsCounterJob(wordsDocsOutput, docsOutput);
        Job tfIdf = buildTfIdfCounterJob(docsOutput, tfIdfOutput);
        aggregator.waitForCompletion(true);
        counter.waitForCompletion(true);
        tfIdf.waitForCompletion(true);
        return 0;
    }

    public static void main(String[] args) throws Exception
    {
        int exitCode = ToolRunner.run(new Runner(), args);
        System.exit(exitCode);
    }
}
