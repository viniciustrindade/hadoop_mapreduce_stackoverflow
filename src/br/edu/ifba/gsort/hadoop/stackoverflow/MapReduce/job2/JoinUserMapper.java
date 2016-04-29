package br.edu.ifba.gsort.hadoop.stackoverflow.MapReduce.job2;


import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public  class JoinUserMapper extends Mapper<Object, Text, Text, Text> {

	private Text outkey = new Text();
	private Text outvalue = new Text();

	@Override
	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {

		// Parse the input string into a nice map
		StringTokenizer tokenizer = new StringTokenizer(value.toString(),"\t");
		String userId = tokenizer.hasMoreTokens() ? tokenizer.nextToken() : null;
	
		
		if (userId == null) {
			return;
		}

		// The foreign join key is the user ID
		outkey.set(userId);

		// Flag this record for the reducer and then output
		outvalue.set("A" + value.toString());
		context.write(outkey, outvalue);
	}
}