package MapReduce;

import java.io.InputStream;
import dsp.tagger.BGUTagDictionary;
import dsp.tagger.TagDictionary;

public class TagDictionarySingleton {
	private TagDictionary dict;
	
	private TagDictionarySingleton()
	{
		try {
			InputStream lexiconStream = TagDictionarySingleton.class.getResourceAsStream("/lexicon");
			InputStream knownBitmasksStream = TagDictionarySingleton.class.getResourceAsStream("/known-bitmasks");
			InputStream swmapStream = TagDictionarySingleton.class.getResourceAsStream("/swmap");
			InputStream noncountNounsStream = TagDictionarySingleton.class.getResourceAsStream("/non-count-nouns");			
			
			this.dict = new BGUTagDictionary(lexiconStream, knownBitmasksStream, swmapStream, noncountNounsStream);
		} catch (Exception e) {
			e.printStackTrace();
			this.dict = null;
		}
	}
	
	private TagDictionary getDictionary()
	{
		return this.dict;
	}
	
	private static class SingletonHolder
	{
		public static final TagDictionarySingleton INSTANCE = new TagDictionarySingleton();
	}
	
	public static TagDictionary getInstance()
	{
		return SingletonHolder.INSTANCE.getDictionary();
	}
}
