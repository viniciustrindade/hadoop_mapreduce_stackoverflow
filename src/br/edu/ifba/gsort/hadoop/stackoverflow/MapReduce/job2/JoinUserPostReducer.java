package br.edu.ifba.gsort.hadoop.stackoverflow.MapReduce.job2;

import java.io.IOException;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class JoinUserPostReducer extends Reducer<Text, Text, Text, Text> {

	TreeMap<Integer,String> map = new TreeMap<Integer,String> ();
	

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		String userInfo = null;
		int sum = 0;

		for (Text t : values) {
			if (t.charAt(0) == 'A') {
				userInfo = t.toString().substring(1);
			} else {
				sum+= Integer.valueOf(t.toString());
			
			}
		}
		if (userInfo != null && sum!=0) {
			map.put(sum, userInfo);
		}
		
		/*if (userInfo != null) {
			context.write(new Text(userInfo),new Text(String.valueOf(count)));
		}*/
	
		


	}
	@Override
	protected void cleanup(Reducer<Text, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		super.cleanup(context);
		
		for(Entry<Integer,String> e:map.entrySet()){
			context.write(new Text(String.valueOf(e.getKey())),new Text(e.getValue()));
			
		}
	}
	
	
	
}