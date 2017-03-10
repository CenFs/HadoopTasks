
package fi.tut.dip;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.util.regex.*;
import java.io.*;
import java.nio.charset.Charset;



public class ProductCount {

    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, IntWritable>{

        private static final String logEntryPattern = "^([\\d.]+) (\\S+) (\\S+) \\[([\\w:/]+\\s[+\\-]\\d{4})\\] \"(.+?)\" (\\d{3}) (\\d+) \\\"([^\"]+)\" \"([^\"]+)\"";
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

	static Pattern r = Pattern.compile(logEntryPattern);

        /*Your Mapper Code here*/
	 public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String line = value.toString();
	Matcher m = r.matcher(line);
	if (m.matches()) {
		//word.set("matches");
		//context.write(word, one);
			String datentime = m.group(4);
			String[] hours = datentime.split(":");
			word.set(hours[1]);
  			context.write(word, one);
		} else {
		word.set("not match");
		context.write(word, one);
		}
           // String[] items = line.split("\\s");

		//String[] sp = line.split(":");
		//word.set(sp[1]);
                //context.write(word, one);
	}
    }

    public static class IntSumReducer
            extends Reducer<Text,IntWritable,Text,IntWritable> {
        private IntWritable result = new IntWritable();

        /*Your Reducer Code here*/
  public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        if (otherArgs.length < 2) {
            System.err.println("Usage: wordcount <in> [<in>...] <out>");
            System.exit(2);
        }

        Job job = Job.getInstance(conf, "Product Count");

        job.setJarByClass(ProductCount.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        for (int i = 0; i < otherArgs.length - 1; ++i) {
            TextInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }

        FileOutputFormat.setOutputPath(job,new Path(otherArgs[otherArgs.length - 1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
