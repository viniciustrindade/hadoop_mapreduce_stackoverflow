package br.edu.ifba.gsort.hadoop.stackoverflow.MapReduce.job1;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import br.edu.ifba.gsort.hadoop.common.Utill;

public class UserMapper extends Mapper<Object, Text, Text, Text> {

	private Text outUserId = new Text();
	

	@Override
	protected void setup(
			Mapper<Object, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		super.setup(context);
	}

	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {

		Map<String, String> parsed = Utill.transformXmlToMap(value.toString());

		final String userId = parsed.get("Id");
		final String location = parsed.get("Location");
		final String displayName = parsed.get("DisplayName");

		if (userId == null ||
				location == null ||
						displayName == null || 
						(!location.toLowerCase().contains("brasil") && 
						!location.toLowerCase().contains("brazil"))){
			return;
		}
		outUserId.set(userId);
		context.write(outUserId, new Text(displayName + "::" + location));
	}



}