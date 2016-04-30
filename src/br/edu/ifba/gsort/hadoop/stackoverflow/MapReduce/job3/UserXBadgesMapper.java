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

public class UserXBadgesMapper extends Mapper<Object, Text, Text, Text> {

	private Text outkey = new Text();
	private Text outvalue = new Text();
	
	@Override
	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {

		Map<String, String> parsed = Utility.transformXmlToMap(value.toString());

		String userId = parsed.get("UserId");
		String creationDate = parsed.get("Date");
		String badge = parsed.get("Id");
		String badgeName = parsed.get("Name");
		String ano = null;
		
		if (Utility.filterIsEmptyNullable(userId,badge,badgeName,creationDate)) {
			return;
		}
		
		ano = Utility.getYear(creationDate);
		
		if (Utility.filterIsEmptyNullable(ano)){
			return;
		}
		
		String val = "[EARN]" +badgeName ;
		


		outkey.set(userId + ano);
		outvalue.set("D" + Utility.SEPARADOR  + ano  + Utility.SEPARADOR + val);
		context.write(outkey, outvalue);
	}



}