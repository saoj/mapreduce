package org.mapreduce;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public class WithMapReduceAndParallelization {
	
	public static void main(String[] args) {
		
		// MAP:
		
		final List<MappedItem> mappedItems = new LinkedList<MappedItem>();
		
		MapCallback mapCallback = new MapCallback() {

			@Override
			public synchronized void mapDone(String filename, List<MappedItem> values) {
				mappedItems.addAll(values);
			}
		};
		
		List<Thread> mapThreads = new LinkedList<Thread>();
		
		mapThreads.add(map("file1.txt", "foo foo bar cat dog dog", mapCallback));
		mapThreads.add(map("file2.txt", "foo house cat cat dog", mapCallback));
		mapThreads.add(map("file3.txt", "foo bird foo foo", mapCallback));
		
		waitForAllThreadsToFinish(mapThreads); // blocking call...
		
		System.out.println(mappedItems);
		
		// GROUP:
		
		Map<String, List<String>> groupedItems = group(mappedItems);
		
		System.out.println(groupedItems);
		
		final Map<String, Map<String, FileMatch>> index = new HashMap<String, 
											Map<String, FileMatch>>();
		
		ReduceCallback reduceCallback = new ReduceCallback() {

			@Override
			public synchronized void reduceDone(String word, 
								Map<String, FileMatch> reducedMap) {
				index.put(word, reducedMap);
			}
		};
		
		List<Thread> reduceThreads = new LinkedList<Thread>();
		
		Iterator<Entry<String, List<String>>> groupedIter = groupedItems.entrySet().iterator();
		
		while(groupedIter.hasNext()) {
			
			Entry<String, List<String>> entry = groupedIter.next();
			
			String word = entry.getKey();
			List<String> list = entry.getValue();
			
			// REDUCE:

			reduceThreads.add(reduce(word, list, reduceCallback));
		}
		
		waitForAllThreadsToFinish(reduceThreads); // blocking call...
		
		printIndex(index);
	}
	
	public static Thread map(final String filename, final String fileContents, final MapCallback mapCallback) {
		
		Thread t = new Thread(new Runnable() {

			@Override
			public void run() {

				List<MappedItem> mappedItems = new LinkedList<MappedItem>();
				
				String[] words = fileContents.split("\\s+");
				
			    for(String word: words) {
			        mappedItems.add(new MappedItem(word, filename));
			    }
			    
			    mapCallback.mapDone(filename, mappedItems);
				
			}
		});
		
		t.start();
		
		return t;
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
	
	public static Thread reduce(final String word, final List<String> list, final ReduceCallback reduceCallback) {
		
		Thread t = new Thread(new Runnable() {

			@Override
			public void run() {
				
				Map<String, FileMatch> reducedMap = new HashMap<String, FileMatch>();
				
				for (String filename : list) {
					
					FileMatch fileMatch = reducedMap.get(filename);
					
					if (fileMatch == null) {
						fileMatch = new FileMatch(filename);
						reducedMap.put(filename, fileMatch);
					}
					
					fileMatch.inc();
				}	
				
				reduceCallback.reduceDone(word, reducedMap);
			}
		});
		
		t.start();
		
		return t;
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
	
	public static void waitForAllThreadsToFinish(List<Thread> threads) {
		try {
			for(Thread t: threads) t.join();
		} catch(InterruptedException e) {
			throw new RuntimeException(e);
		}
	}
}