package br.edu.ifba.gsort.hadoop.stackoverflow.MapReduce.job2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class JoinUserPostReducer extends Reducer<Text, Text, Text, Text> {

	private TreeMap<UserXPost, String> map = new TreeMap<UserXPost, String>(new Comparator<UserXPost>() {
		public int compare(UserXPost o1, UserXPost o2) {
			if (o1 instanceof UserXPost && o2 instanceof UserXPost) {
				int retorno = o1.getPostCount().compareTo(o2.getPostCount());
				if (retorno == 0) {
					return o1.getDisplayName().compareTo(o2.getDisplayName());
				} else {
					return retorno;
				}
			} else {
				return 0;
			}
		}
	});

	private ArrayList<String> listA = new ArrayList<String>();
	private ArrayList<String> listB = new ArrayList<String>();

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {


		// Clear our lists
		listA.clear();
		listB.clear();

		for (Text t : values) {
			if (t.charAt(0) == 'A') {
				listA.add(t.toString().substring(1));
			} else if (t.charAt(0) == 'B') {
				listB.add(t.toString().substring(1));

			}
		}

		if (!listA.isEmpty() && !listB.isEmpty()) {
			for (String A : listA) {
				int postCount = 0;
				for (String B : listB) {
					postCount += Integer.valueOf(B);
				}
				UserXPost userxPost = new UserXPost(key.toString(),A, postCount);

				map.put(userxPost, String.valueOf(postCount));
				if (map.size() > 10) {
					try{
					map.remove(map.firstEntry());
					}catch(Exception e){}
				}
			}
		}

	}

	@Override
	protected void cleanup(Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
		super.cleanup(context);

		for (Entry<UserXPost, String> e : map.descendingMap().entrySet()) {
			context.write(new Text(String.valueOf(e.getKey().getDisplayName())), new Text(e.getValue()));

		}
	}

}