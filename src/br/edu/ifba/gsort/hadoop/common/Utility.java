package br.edu.ifba.gsort.hadoop.common;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class Utility {
	public static String SEPARADOR = "\t";
	
	public static Map<String, String> transformXmlToMap(String xml) {
		Map<String, String> map = new HashMap<String, String>();
		try {
			String[] tokens = xml.trim().substring(5, xml.trim().length() - 3)
					.split("\"");

			for (int i = 0; i < tokens.length - 1; i += 2) {
				String key = tokens[i].trim();
				String val = tokens[i + 1];

				map.put(key.substring(0, key.length() - 1), val);
			}
		} catch (StringIndexOutOfBoundsException e) {
			System.err.println(xml);
		}

		return map;
	}
	/**
	 * Formats the date as YEAR
	 * Example: 2010-05-28T13:43:13.617 will return 2010
	 * @param creation
	 * @return
	 */
	public static String getYear(String creation){
		DateFormat format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
		DateFormat format2 = new SimpleDateFormat("yyyy");
		try {
			Date data = format.parse(creation);
			return format2.format(data);
		} catch (ParseException e) {
			return null;
		}
	}
	public static String getYearMonth(String creation){
		DateFormat format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
		DateFormat format2 = new SimpleDateFormat("yyyy/MM");
		try {
			Date data = format.parse(creation);
			return format2.format(data);
		} catch (ParseException e) {
			return null;
		}
	}
	
	public static boolean filterIsEmptyNullable(Object... objects){
		for (Object object : objects) {
			if (object == null)
				return true;
			
			if (String.valueOf(object).isEmpty())
				return true;
		}
		return false;
	}
	
	public static boolean filterIsEnqualIgnoreCase(String object1,String object2){

			
			return !filterIsEmptyNullable(object1,object2) && object1.toLowerCase().contains(object2);
	}
}
