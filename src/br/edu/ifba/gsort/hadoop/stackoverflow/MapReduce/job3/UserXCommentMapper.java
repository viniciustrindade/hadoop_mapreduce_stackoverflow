package br.edu.ifba.gsort.hadoop.stackoverflow.MapReduce.job3;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import br.edu.ifba.gsort.hadoop.common.Utill;

public class UserXCommentMapper extends Mapper<Object, Text, Text, Text> {

	private Text outkey = new Text();
	private Text outvalue = new Text();
	
	@Override
	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {

		Map<String, String> parsed = Utill.transformXmlToMap(value.toString());


		String userId = parsed.get("UserId");
		String creationDate = parsed.get("CreationDate");
		String commentId = parsed.get("Id");
		String ano = null;
		
		if (userId == null || commentId == null || creationDate == null) {
			return;
		}

		if (Utill.filterIsEmptyNullable(userId,commentId,creationDate)) {
			return;
		}
		
		ano = Utill.getYear(creationDate);
		
		if (Utill.filterIsEmptyNullable(ano)){
			return;
		}
		String val = "[COMMENTED]" + commentId;
		


		outkey.set(userId + ano);
		outvalue.set("C" +val);
		context.write(outkey, outvalue);
	}



}