package br.edu.ifba.gsort.hadoop.stackoverflow.MapReduce.job1;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import br.edu.ifba.gsort.hadoop.common.Utility;

public class UserFilteringMapper extends
		Mapper<Object, Text, NullWritable, Text> {

	private Text outUserId = new Text();

	@Override
	protected void setup(
			Mapper<Object, Text, NullWritable, Text>.Context context)
			throws IOException, InterruptedException {
		super.setup(context);
	}

	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {

		Map<String, String> parsed = Utility.transformXmlToMap(value.toString());

		final String userId = parsed.get("Id");
		final String location = parsed.get("Location");

		if (Utility.filterIsEmptyNullable(userId, location)
				|| (!Utility.filterIsEnqualIgnoreCase(location,"brasil") 
				&& !Utility.filterIsEnqualIgnoreCase(location,"brazil"))
			) {
			return;
		}
		outUserId.set(userId);
		context.write(NullWritable.get(), value);
	}

}