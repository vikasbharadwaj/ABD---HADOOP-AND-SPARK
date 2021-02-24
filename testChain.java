import java.io.IOException;
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


public class testChain {
	
	public static class filterMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
					String line = value.toString();
					String state = line.substring(0,3);
					if (state.equals("KAR")){
						context.write(value, new IntWritable(1));
					}					
		}
	}

	
	public static class filterReducer extends Reducer <Text, IntWritable, Text, IntWritable > {
		public void reduce(Text key, Iterable<IntWritable> values, Context context) 
			throws IOException, InterruptedException {
				context.write(key, new IntWritable(1) );
		}
	}
	
	public static class groupMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
					String line = value.toString();
					String[] words = line.split("\t");
					int age = Integer.parseInt(words[6]);
					String age_group = new String();
					if (age < 10){
						age_group = "0-9";
					} else if (age > 9 && age < 20) {
						age_group = "10-19"; 
					}else if (age >19 && age < 40) {
						age_group = "20-39";
					}else if (age > 39 && age < 75) {
						age_group = "40-74";
					}else {
						age_group = "75+";
					}
					value.set(age_group);
					context.write(value, new IntWritable(1));
										
		}
	}

	
	public static class groupReducer extends Reducer <Text, IntWritable, Text, IntWritable > {
		public void reduce(Text key, Iterable<IntWritable> values, Context context) 
			throws IOException, InterruptedException {
			
			int sum = 0;
			for (IntWritable x: values) {
				sum += x.get();
			}
			context.write(key, new IntWritable(sum) );
		}
	}

	public static void main(String[] args) throws Exception{
		// TODO Auto-generated method stub
		
		Configuration conf1 = new Configuration();
		
		Job job = Job.getInstance(conf1, "Age count");

		job1.setJarByClass(testChain.class);
		job1.setMapperClass(filterMapper.class);
		job1.setReducerClass(filterReducer.class);

		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(IntWritable.class);

		FileInputFormat.addInputPath(job1, new Path(args[0]));		
		FileOutputFormat.setOutputPath(job1, new Path(args[1]));
		job1.waitForCompletion(true);
		
		
		Configuration conf2 = new Configuration();
		
		Job job2 = Job.getInstance(conf2, "Age count");

		job2.setJarByClass(testChain.class);
		job2.setMapperClass(groupMapper.class);
		job2.setReducerClass(groupReducer.class);

		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(IntWritable.class);

		FileInputFormat.addInputPath(job2, new Path(args[1]));		
		FileOutputFormat.setOutputPath(job2, new Path(args[2]));

		System.exit(job2.waitForCompletion(true) ? 0: 1);

	}

}
