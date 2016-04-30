package br.edu.ifba.gsort.hadoop.stackoverflow.MapReduce.job3;

import java.io.IOException;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import br.edu.ifba.gsort.hadoop.common.Utility;

public class UserMapper extends Mapper<Object, Text, Text, Text> {

	private Text outkey = new Text();
	private Text outvalue = new Text();

	@Override
	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {

		Map<String, String> parsed = Utility.transformXmlToMap(value.toString());

		final String userId = parsed.get("Id");
		final String displayName = parsed.get("DisplayName");
		final String creationDate = parsed.get("CreationDate");
		String ano = null;

		if (Utility.filterIsEmptyNullable(userId, displayName)) {
			return;
		}

		ano = Utility.getYear(creationDate);
		if (Utility.filterIsEmptyNullable(ano)) {
			return;
		}

		String val = "[SIGN-UP]";

		outkey.set(userId + ano);
		outvalue.set("A" + userId + Utility.SEPARADOR + displayName.toString()
				 +"\n" + Utility.SEPARADOR  + ano  + Utility.SEPARADOR + val);
		context.write(outkey, outvalue);
	}
}