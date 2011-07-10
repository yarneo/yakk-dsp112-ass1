
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
	            TagDictionary tagger = new BGUTagDictionary("lexicon.txt","known-bitmasks.txt","swmap.txt","non-count-nouns.txt");
	            System.out.println("בצלם: " + tagger.getTagsForWord("בצלם"));
	            System.out.println();
	            System.out.println("Similar-words based distribution:");
	            for (Analysis<String> analysis : tagger.getTagsDistributionForWord("בצלם",false)) 
	                System.out.println("\t" + analysis.getTag() + ": " + analysis.getProb());
	            System.out.println();
	            System.out.println("Uniform distribution:");
	            for (Analysis<String> analysis : tagger.getTagsDistributionForWord("בצלם",true)) 
	                System.out.println("\t" + analysis.getTag() + ": " + analysis.getProb());
	        } catch (Exception e) { 
	            e.printStackTrace();  
	        }
	    }
	}

