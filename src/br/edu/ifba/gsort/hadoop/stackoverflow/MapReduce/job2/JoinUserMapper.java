package br.edu.ifba.gsort.hadoop.stackoverflow.MapReduce.job2;


import java.io.IOException;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import br.edu.ifba.gsort.hadoop.common.Utility;

public  class JoinUserMapper extends Mapper<Object, Text, Text, Text> {

	private Text outkey = new Text();
	private Text outvalue = new Text();

	@Override
	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {

		Map<String, String> parsed = Utility.transformXmlToMap(value.toString());
		
		final String userId = parsed.get("Id");
		final String displayName = parsed.get("DisplayName");

		if (Utility.filterIsEmptyNullable(userId, displayName)) {
			return;
		}

		// The foreign join key is the user ID
		outkey.set(userId);

		// Flag this record for the reducer and then output
		outvalue.set("A" + displayName.toString());
		context.write(outkey, outvalue);
	}
}