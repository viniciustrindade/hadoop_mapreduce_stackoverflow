package br.edu.ifba.gsort.hadoop.stackoverflow.MapReduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobStatus;
import org.apache.hadoop.mapreduce.Job;

public class JobWrapper extends Job {

	public JobWrapper(Job job) throws IOException {

	}

	public JobWrapper(Configuration conf) throws IOException {
		super(conf);
		// TODO Auto-generated constructor stub
	}

	public JobWrapper(Configuration conf, String jobName) throws IOException {
		super(conf, jobName);
		// TODO Auto-generated constructor stub
	}

	public JobWrapper waitFor(JobWrapper dependent) throws IOException,
			InterruptedException, ClassNotFoundException {
		
		if (!dependent.waitForCompletion(false)) {
			System.exit(0);
		}
	
		if (!this.waitForCompletion(false)) {
			System.exit(0);
		}
		return this;
	}
	
	@Override
	public String toString() {

		return this.getJobName();
	}

}
