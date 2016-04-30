package br.edu.ifba.gsort.hadoop.stackoverflow.MapReduce.job3;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import br.edu.ifba.gsort.hadoop.common.Utility;

public class UserXPostMapper extends Mapper<Object, Text, Text, Text> {

	private Text outkey = new Text();
	private Text outvalue = new Text();
	
	@Override
	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {

		Map<String, String> parsed = Utility.transformXmlToMap(value.toString());
		
		

		String userId = parsed.get("OwnerUserId");
		String creationDate = parsed.get("CreationDate");
		String postId = parsed.get("Id");
		String ano = null;
		

		if (Utility.filterIsEmptyNullable(userId,postId,creationDate)) {
			return;
		}

		ano = Utility.getYear(creationDate);
		
		if (Utility.filterIsEmptyNullable(ano)){
			return;
		}
		
		String val = "[POSTED]" + postId;
		


		outkey.set(userId + ano);
		outvalue.set("B" + Utility.SEPARADOR  + ano  + Utility.SEPARADOR + val);
		context.write(outkey, outvalue);
	}



}