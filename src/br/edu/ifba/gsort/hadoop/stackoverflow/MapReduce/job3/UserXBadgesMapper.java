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

public class UserXBadgesMapper extends Mapper<Object, Text, Text, Text> {

	private Text outkey = new Text();
	private Text outvalue = new Text();
	
	@Override
	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {

		Map<String, String> parsed = Utill.transformXmlToMap(value.toString());
		//2010-05-28T13:43:13.617
		

		String userId = parsed.get("UserId");
		String creationDate = parsed.get("Date");
		String badge = parsed.get("Id");
		String badgeName = parsed.get("Name");
		String dia = null;
		
		if (userId == null || badge == null  || badgeName == null|| creationDate == null) {
			return;
		}

		DateFormat format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
		DateFormat format2 = new SimpleDateFormat("yyyy");
		try {
			Date data = format.parse(creationDate);
			dia = format2.format(data);
		} catch (ParseException e) {
			return;
		}
		
		String val = ":: EARN: " + badge + "["  +badgeName + "]";
		
		
		outkey.set(dia +"\t"+userId);
		outvalue.set(val);
		context.write(outkey, outvalue);
	}



}