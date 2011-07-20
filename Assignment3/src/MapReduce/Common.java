package MapReduce;
import dsp.tagger.AnalysisException;
import dsp.tagger.TagDictionary;


public class Common {
	public static double allow(String tag, String word)
	{
		TagDictionary tagger = TagDictionarySingleton.getInstance();
		
		try {
			// TODO: "-" prefix in tags from getTagsForWord.
			if (tagger.getTagsForWord(word).contains(tag) || tagger.getTagsForWord(word).contains("-" + tag)) {
				return 1;
			} else {
				return 0;
			}
		} catch (AnalysisException ae) {
			ae.printStackTrace();
			return 0;
		}				
	}
}
