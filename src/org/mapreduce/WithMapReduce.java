package org.mapreduce;

import java.util.List;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;

public class WithMapReduce {
	
	public static void main(String[] args) {
		
		// MAP:
		
		List<MappedItem> mappedItems = new LinkedList<MappedItem>();
		
		mappedItems.addAll(map("file1.txt", "foo foo bar cat dog dog"));
		mappedItems.addAll(map("file2.txt", "foo house cat cat dog"));
		mappedItems.addAll(map("file3.txt", "foo bird foo foo"));
		
		System.out.println(mappedItems);
		
		// GROUP:
		
		Map<String, List<String>> groupedItems = group(mappedItems);
		
		System.out.println(groupedItems);
		
		Map<String, Map<String, FileMatch>> index = new HashMap<String, Map<String, FileMatch>>();
		
		Iterator<Entry<String, List<String>>> groupedIter = groupedItems.entrySet().iterator();
		
		while(groupedIter.hasNext()) {
			
			Entry<String, List<String>> entry = groupedIter.next();
			
			String word = entry.getKey();
			List<String> list = entry.getValue();
			
			// REDUCE:

			Map<String, FileMatch> reducedMap = reduce(word, list);
			
			index.put(word, reducedMap);
		}
		
		printIndex(index);
	}
	
	public static List<MappedItem> map(final String filename, final String fileContents) {
		
		List<MappedItem> mappedItems = new LinkedList<MappedItem>();
		
		String[] words = fileContents.split("\\s+");
		
	    for(String word: words) {
	        mappedItems.add(new MappedItem(word, filename));
	    }
	    
	    return mappedItems;
	}
	
	public static Map<String, List<String>> group(List<MappedItem> mappedItems) {
		
		Map<String, List<String>> groupedItems = new HashMap<String, List<String>>();
		 
		Iterator<MappedItem> iter = mappedItems.iterator();
		
		while(iter.hasNext()) {
			
		    MappedItem item = iter.next();
		    
		    String word = item.getWord();
		    String file = item.getFile();
		    
		    List<String> list = groupedItems.get(word);
		    
		    if (list == null) {
		        list = new LinkedList<String>();
		        groupedItems.put(word, list);
		    }
		    
		    list.add(file);
		}
		
		return groupedItems;
	}
	
	public static Map<String, FileMatch> reduce(String word, List<String> list) {
		
		Map<String, FileMatch> reducedMap = new HashMap<String, FileMatch>();
		
		for (String filename : list) {
			
			FileMatch fileMatch = reducedMap.get(filename);
			
			if (fileMatch == null) {
				fileMatch = new FileMatch(filename);
				reducedMap.put(filename, fileMatch);
			}
			
			fileMatch.inc();
		}
		
		return reducedMap;
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