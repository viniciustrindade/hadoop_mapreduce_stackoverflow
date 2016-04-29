package br.edu.ifba.gsort.hadoop.stackoverflow.MapReduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import br.edu.ifba.gsort.hadoop.stackoverflow.MapReduce.job1.UserMapper;
import br.edu.ifba.gsort.hadoop.stackoverflow.MapReduce.job2.JoinPostMapper;
import br.edu.ifba.gsort.hadoop.stackoverflow.MapReduce.job2.JoinUserMapper;
import br.edu.ifba.gsort.hadoop.stackoverflow.MapReduce.job2.JoinUserPostReducer;
import br.edu.ifba.gsort.hadoop.stackoverflow.MapReduce.job3.UserXBadgesMapper;
import br.edu.ifba.gsort.hadoop.stackoverflow.MapReduce.job3.UserXCommentMapper;
import br.edu.ifba.gsort.hadoop.stackoverflow.MapReduce.job3.UserXPostMapper;

public class Main {

	public static void main(String[] args) throws IOException,
			InterruptedException, ClassNotFoundException {

		Path out = new Path(args[0]);
		Path outJob1 = new Path(out + "_job1_users");
		Path outJob2 = new Path(out + "_job2_top10");
		Path outJob3 = new Path(out + "_job3_historic");
		Path userPath = new Path(args[1]);
		Path bagesPath = new Path(args[2]);
		Path postPath = new Path(args[3]);
		Path commentsPath = new Path(args[4]);

		// Create configuration
		Configuration conf = new Configuration(true);

		JobWrapper job1 = job1(conf, userPath, outJob1);
		JobWrapper job2 = job2(conf, outJob1, postPath, outJob2);
		JobWrapper job3 = job3(conf, userPath, postPath, commentsPath,
				bagesPath, outJob3);
		if (job3.waitFor(job2.waitFor(job1)).isSuccessful()) {
			;
			System.exit(0);
		}else{
			System.exit(1);
			
		}

	}

	public static JobWrapper job1(Configuration conf, Path userXMLPath, Path out)
			throws IOException, ClassNotFoundException, InterruptedException {
		FileSystem.get(conf).delete(out, true);

		// Create job
		JobWrapper job1 = new JobWrapper(conf, "job1");
		job1.setJarByClass(UserMapper.class);

		// Setup Map
		job1.setMapperClass(UserMapper.class);
		job1.setNumReduceTasks(0);

		// Specify key / value
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job1, userXMLPath);
		job1.setInputFormatClass(TextInputFormat.class);

		// Output
		FileOutputFormat.setOutputPath(job1, out);
		job1.setOutputFormatClass(TextOutputFormat.class);

		return job1;

	}

	public static JobWrapper job2(Configuration conf, Path user, Path post,
			Path out) throws IOException, ClassNotFoundException,
			InterruptedException {
		FileSystem.get(conf).delete(out, true);

		// Create job
		JobWrapper job2 = new JobWrapper(conf, "job2");
		job2.setJarByClass(JoinUserMapper.class);

		// Setup MapReduce
		MultipleInputs.addInputPath(job2, user, TextInputFormat.class,
				JoinUserMapper.class);
		MultipleInputs.addInputPath(job2, post, TextInputFormat.class,
				JoinPostMapper.class);
		//job2.setCombinerClass(JoinUserPostReducer.class);
		job2.setReducerClass(JoinUserPostReducer.class);
		job2.setNumReduceTasks(1);

		// Specify key / value
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);

		// Output
		FileOutputFormat.setOutputPath(job2, out);
		job2.setOutputFormatClass(TextOutputFormat.class);

		return job2;
	}

	public static JobWrapper job3(Configuration conf, Path user, Path post,
			Path comment, Path badge, Path out) throws IOException,
			ClassNotFoundException, InterruptedException {
		FileSystem.get(conf).delete(out, true);
		// Create job
		JobWrapper job3 = new JobWrapper(conf, "job3");
		job3.setJarByClass(UserXPostMapper.class);

		// Setup MapReduce
		MultipleInputs.addInputPath(job3, post, TextInputFormat.class,
				UserXPostMapper.class);
		MultipleInputs.addInputPath(job3, comment, TextInputFormat.class,
				UserXCommentMapper.class);
		MultipleInputs.addInputPath(job3, badge, TextInputFormat.class,
				UserXBadgesMapper.class);
		// job3Join.setNumReduceTasks(0);
		// job3Join.setReducerClass(UserXPostCombiner.class);
		// job3Join.setNumReduceTasks(1);

		// Specify key / value
		job3.setOutputKeyClass(Text.class);
		job3.setOutputValueClass(Text.class);

		// Output
		FileOutputFormat.setOutputPath(job3, out);
		job3.setOutputFormatClass(TextOutputFormat.class);

		return job3;
	}

}