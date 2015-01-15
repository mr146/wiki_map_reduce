package wiki;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
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

    private Job buildWordsAggregatorJob(String input, String output) throws IOException
    {

        Job job = Job.getInstance();
        job.setJobName("words_aggregator");
        job.setJarByClass(getClass());
        job.setNumReduceTasks(1);

        job.setInputFormatClass(KeyValueTextInputFormat.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(WikiWordsAggregatorMapper.class);
        job.setReducerClass(WikiWordsAggregatorReducer.class);

        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));
        return job;
    }

    private Job buildWordsCounterJob(String input, String output) throws IOException
    {
        Job job = Job.getInstance();
        job.setJobName("skiplist_builder");
        job.setJarByClass(getClass());
        job.setNumReduceTasks(1);

        job.setInputFormatClass(KeyValueTextInputFormat.class);

        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(WikiWordsCounterMapper.class);
        job.setReducerClass(WikiWordsCounterReducer.class);

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

        String aggregatorOutput = "wiki/skiplist/aggregate";
        String skiplistOutput = "wiki/skiplist/skiplist";

        Job aggregator = buildWordsAggregatorJob(args[0], aggregatorOutput);
        Job counter = buildWordsCounterJob(aggregatorOutput, skiplistOutput);
        aggregator.waitForCompletion(true);
        counter.waitForCompletion(true);
        return 0;
    }

    public static void main(String[] args) throws Exception
    {
        int exitCode = ToolRunner.run(new Runner(), args);
        System.exit(exitCode);
    }
}
