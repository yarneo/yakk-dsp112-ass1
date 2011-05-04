package MapReduce1;

/*
 * HadoopMapper.java
 *
 * Created on Apr 27, 2011, 8:01:47 PM
 */



import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author yarneo
 */



public class SubSequencesMapper extends Mapper<Text, LongWritable, Text, UserWritable>{
	
	public static ArrayList<ArrayList<String>> powerList( ArrayList<String> OriginalList) {
		ArrayList<ArrayList<String>> lists1 = new ArrayList<ArrayList<String>>();
		if (OriginalList.isEmpty()) {
			lists1.add(new ArrayList<String>());
			return lists1;
		}
		List<String> list = new ArrayList<String>(OriginalList);
		String head = list.get(0);
		ArrayList<String> rest = new ArrayList<String>(list.subList(1, list.size())); 
		for (ArrayList<String> list1 : powerList(rest)) {
			ArrayList<String> newList = new ArrayList<String>();
			newList.add(head);
			newList.addAll(list1);
			lists1.add(newList);
			lists1.add(list1);
		}           
		return lists1;
	}




	public void map(Text key, LongWritable value, Context context) throws IOException, InterruptedException {
		Text word = new Text();
		LongWritable freqs = new LongWritable();
		LongWritable ctxtFreq = new LongWritable();
				
		long freq = value.get();

		StringTokenizer itr = new StringTokenizer(key.toString());
		ArrayList<String> words = new ArrayList<String>();
		while (itr.hasMoreTokens()) {
			words.add(itr.nextToken());            
		}
		for (ArrayList<String> s : powerList(words)) {
			if(s.size()!=0) {
				String tmp ="";
				String tmp2="";
				int index=0;
				ArrayList<String> contextAL = new ArrayList<String>();
				for(int j=0;j<s.size();j++) {
					if(j==s.size()-1) {
						tmp += s.get(j);
					}
					else {
						tmp += s.get(j) + " ";
					}
					for(int i=index;i<words.size();i++) {
						if(s.get(j).equals(words.get(i)) && (j!=s.size()-1)) {
							index=i+1;
							break;
						}
						else if(s.get(j).equals(words.get(i)) && (j==s.size()-1)) {
							for(int k=i+1;k<words.size();k++) {
								contextAL.add(words.get(k));
							}
							break;
						}
						else {
							contextAL.add(words.get(i));
						}
					}

				}

				for(int k=0;k<contextAL.size();k++) {
					String strTemp = contextAL.get(k);
				    if(!Stopwords.isStopword(strTemp)) {
				    	if(k == contextAL.size()-1) {
				    		if(strTemp.endsWith("\"")) {
				    			strTemp = strTemp.substring(0, strTemp.length()-1);
				    		}
				    		tmp2 += strTemp;
				    	}
				    	else {
				    		tmp2 += strTemp + " ";
				    	}
				    }
				}
				word.set(tmp);
				freqs.set(freq);
				ctxtFreq.set(freq);
				tmp2 = tmp2.trim();
				ContextsUserWritable cuw = new ContextsUserWritable(new Text(tmp2),ctxtFreq);
				ContextsUserWritable[] cuwArr = {cuw};
				UserArrayWritable ctxts = new UserArrayWritable(cuwArr);
				UserWritable user = new UserWritable(freqs,ctxts);
				context.write(word, user);
			}
		}
	}
}

