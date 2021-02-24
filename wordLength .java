package wordlength;
import java.io.IOException;
import java.lang.*;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class wordLength {
	
	public static class lengthMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
		
		private IntWritable wlength = new IntWritable();
		private Text word = new Text();
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
					String line = value.toString();
					StringTokenizer tokenizer = new StringTokenizer(line);
					
					while (tokenizer.hasMoreTokens() ) {
						String str = tokenizer.nextToken();
						wlength = new IntWritable(str.length());
						value.set(str);
						
						context.write(wlength, value);
					}
		}
	}

	
	public static class lengthReducer extends Reducer <IntWritable, Text, IntWritable, Text > {
		public void reduce(IntWritable key, Iterable<Text> values, Context context) 
			throws IOException, InterruptedException {
				int sum = 0;
				for (Text x: values) {
					sum++;
				}
				context.write(key, new Text(String.valueOf(sum)));
		}
	}


	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		
		Configuration conf = new Configuration();
		
		Job job = Job.getInstance(conf, "word length");

		job.setJarByClass(wordLength.class);
		job.setMapperClass(lengthMapper.class);
		job.setReducerClass(lengthReducer.class);

		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));		
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0: 1);

		

	}

}
