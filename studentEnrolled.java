import java.io.IOException;
//import java.util.StringTokenizer;

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


public class studentEnrolled {
	public static class filterMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
					String line = value.toString();
					String[] words = line.split(" ");
					String instName = words[3];;
					String institutionName = context.getConfiguration().get("institutionName").toString();
					if (instName.equals(institutionName)){
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
					String[] words = line.split(" ");
					String gender = new String();
					gender = words[1];
					String programName = new String();
					programName = words[0];
					
					if (gender == "M" && programName == "BDA"){
						value.set((programName + '-' + gender));
						context.write(value, new IntWritable(1));
					} else if  (gender == "F" && programName == "BDA") {
						value.set((programName + '-' + gender));
					    context.write(value, new IntWritable(1));
					} else if (gender == "M" && programName == "HDA"){
							value.set((programName + '-' + gender));
							context.write(value, new IntWritable(1));
					} else if (gender == "F" && programName == "HDA"){
						value.set((programName + '-' + gender));
						context.write(value, new IntWritable(1));
				    } else if (gender == "M" && programName == "ML"){
					value.set((programName + '-' + gender));
					context.write(value, new IntWritable(1));
			       } else if (gender == "F" && programName == "ML"){
				    value.set((programName + '-' + gender));
				    context.write(value, new IntWritable(1));
		         }
					
					
										
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
		
		Configuration conf = new Configuration();
		conf.set("institutionName", args[3]));
		
		Job job = Job.getInstance(conf, "my count");

		job.setJarByClass(studentEnrolled.class);
		job.setMapperClass(filterMapper.class);
		job.setReducerClass(filterReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
                job.waitForCompletion(true);

                Configuration conf2 = new Configuration();
		
		Job job2 = Job.getInstance(conf2, "M or F count");

		job2.setJarByClass(studentEnrolled.class);
		job2.setMapperClass(groupMapper.class);
		job2.setReducerClass(groupReducer.class);

		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(IntWritable.class);

		FileInputFormat.addInputPath(job2, new Path(args[1])));		
		FileOutputFormat.setOutputPath(job2, new Path(args[2]));


		System.exit(job.waitForCompletion(true) ? 0: 1);
	}
}