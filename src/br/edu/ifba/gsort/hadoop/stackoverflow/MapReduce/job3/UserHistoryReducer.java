package br.edu.ifba.gsort.hadoop.stackoverflow.MapReduce.job3;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.sun.tools.corba.se.idl.toJavaPortable.Util;

import br.edu.ifba.gsort.hadoop.common.Utill;

public class UserHistoryReducer extends Reducer<Text, Text, Text, Text> {


	private ArrayList<String> listA = new ArrayList<String>();
	private ArrayList<String> listB = new ArrayList<String>();
	private ArrayList<String> listC = new ArrayList<String>();
	private ArrayList<String> listD = new ArrayList<String>();


	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {


		// Clear our lists
		listA.clear();
		listB.clear();
		listC.clear();
		listD.clear();

		for (Text t : values) {
			if (t.charAt(0) == 'A') {
				listA.add(t.toString().substring(1));
			} else if (t.charAt(0) == 'B') {
				listB.add(t.toString().substring(1));

			} else if (t.charAt(0) == 'C') {
				listC.add(t.toString().substring(1));

			} else if (t.charAt(0) == 'D') {
				listD.add(t.toString().substring(1));

			}
		}
		
		for (String A : listA) {
			String retorno = "";
					
			if (!listB.isEmpty()) {
				for (String B : listB) {
					retorno += Utill.SEPARADOR + B;
				}
			}
			if (!listC.isEmpty()) {
				for (String C : listC) {
					retorno += Utill.SEPARADOR +  C;
				}
			}
			if (!listD.isEmpty()) {
				for (String D : listD) {
					retorno += Utill.SEPARADOR +  D;
				}
			}
			if (!Utill.filterIsEmptyNullable(retorno)){
				context.write(new Text(A), new Text(retorno));
			}
		}

	

	}

	
}