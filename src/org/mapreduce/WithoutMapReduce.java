package org.mapreduce;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

public class WithoutMapReduce {
	
	public static void main(String[] args) {
		
		Map<String, Map<String, FileMatch>> index = new HashMap<String, Map<String, FileMatch>>();
		
		addToIndex("file1.txt", "foo foo bar cat dog dog", index);
		addToIndex("file2.txt", "foo house cat cat dog", index);
		addToIndex("file3.txt", "foo bird foo foo", index);
		
		printIndex(index);
	}
	
	public static void addToIndex(final String filename, final String fileContents, final Map<String, Map<String, FileMatch>> index) {
		
		String[] words = fileContents.split("\\s+");
		
		for(String word: words) {
			
			Map<String, FileMatch> fileMatches = index.get(word);
			
			if (fileMatches == null) {
				fileMatches = new HashMap<String, FileMatch>();
				index.put(word, fileMatches);
			}
			
			FileMatch fileMatch = fileMatches.get(filename);
			
			if (fileMatch == null) {
				fileMatch = new FileMatch(filename);
				fileMatches.put(filename, fileMatch);
			}
			
			fileMatch.inc();
		}
	}
	
	public static void printIndex(Map<String, Map<String, FileMatch>> index) {
		
		Iterator<Entry<String, Map<String, FileMatch>>> iter1 = index.entrySet().iterator();
		
		while(iter1.hasNext()) {
			
			Entry<String, Map<String, FileMatch>> entry1 = iter1.next();
			
			String word = entry1.getKey();
			Collection<FileMatch> fileMatches = entry1.getValue().values();
			
			System.out.print(word + " => [ ");
			
			boolean firstElement = true;
			
			for(FileMatch fileMatch: fileMatches) {
				if (firstElement) {
					firstElement = false;
				} else {
					System.out.print(", ");
				}
				System.out.print(fileMatch);
			}
			
			System.out.println(" ]");
		}
	}
}