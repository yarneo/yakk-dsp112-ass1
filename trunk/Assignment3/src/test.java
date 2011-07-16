
	import java.io.InputStream;

import MapReduce.TagDictionarySingleton;
import dsp.tagger.TagDictionary;
	import dsp.tagger.BGUTagDictionary;
import dsp.tagger.Analysis;
	 
	 
	public class test {
	    public static void main(String args[]) {
	        
//	        if (args.length < 3) {
//	            System.out.println(
//	                    "Usage: java -cp bgu-tagger.jar BGUTagDictionary <lexicon file> <bitmasks file> <similar words file>"
//	                    );
//	            System.exit(0);
//	        }
	 
	        try {
	        	InputStream lexiconStream = TagDictionarySingleton.class.getResourceAsStream("/lexicon");
				InputStream knownBitmasksStream = TagDictionarySingleton.class.getResourceAsStream("/known-bitmasks");
				InputStream swmapStream = TagDictionarySingleton.class.getResourceAsStream("/swmap");
				InputStream noncountNounsStream = TagDictionarySingleton.class.getResourceAsStream("/non-count-nouns");
				
	            TagDictionary tagger = new BGUTagDictionary(lexiconStream, knownBitmasksStream, swmapStream, noncountNounsStream);
	            
	            String word = "ספר";
	            //String word = "!\"";
	            
	            //System.out.println(word + ": " + tagger.getTagsForWord(word));
	            System.out.println("Tags for " + word + ": ");
	            for (String t : tagger.getTagsForWord(word)) {
	            	System.out.println("\t" + t);
	            }
	            System.out.println();
	            System.out.println("Similar-words based distribution:");
	            for (Analysis<String> analysis : tagger.getTagsDistributionForWord(word,false)) 
	                System.out.println("\t" + analysis.getTag() + ": " + analysis.getProb());
	            System.out.println();
	            System.out.println("Uniform distribution:");
	            for (Analysis<String> analysis : tagger.getTagsDistributionForWord(word,true)) 
	                System.out.println("\t" + analysis.getTag() + ": " + analysis.getProb());
	        } catch (Exception e) { 
	            e.printStackTrace();  
	        }
	    }
	}

