package br.edu.ifba.gsort.hadoop.stackoverflow.MapReduce.job3;

import java.io.IOException;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import br.edu.ifba.gsort.hadoop.common.Utill;

public class UserMapper extends Mapper<Object, Text, Text, Text> {

	private Text outkey = new Text();
	private Text outvalue = new Text();

	@Override
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

		Map<String, String> parsed = Utill.transformXmlToMap(value.toString());

		final String userId = parsed.get("Id");
		final String displayName = parsed.get("DisplayName");
		final String creationDate = parsed.get("CreationDate");
		String ano = null;

		if (Utill.filterIsEmptyNullable(userId,displayName)) {
			return;
		}

		ano = Utill.getYear(creationDate);
		if (Utill.filterIsEmptyNullable(ano)) {
			return;
		}

		String val = "[SINGUP]";
		
		outkey.set(userId + ano);
		outvalue.set("A" + +userId + Utill.SEPARADOR  + displayName.toString() +"[" +ano + "]"+  Utill.SEPARADOR + val);
		context.write(outkey, outvalue);
	}
}