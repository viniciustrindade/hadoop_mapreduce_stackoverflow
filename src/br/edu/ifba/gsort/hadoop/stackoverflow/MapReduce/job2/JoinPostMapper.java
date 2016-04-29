package br.edu.ifba.gsort.hadoop.stackoverflow.MapReduce.job2;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import br.edu.ifba.gsort.hadoop.common.Utill;

public class JoinPostMapper extends Mapper<Object, Text, Text, Text> {

	private Text outkey = new Text();
	private Text outvalue = new Text();

	@Override
	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {

		Map<String, String> parsed = Utill.transformXmlToMap(value.toString());

		String userId = parsed.get("OwnerUserId");
		if (userId == null) {
			return;
		}

		outkey.set(userId);
		outvalue.set(String.valueOf(1));
		context.write(outkey, outvalue);

	}
	
}
