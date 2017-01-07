package org.mapreduce.dumbo.wordcount;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.mapreduce.dumbo.Reducer;
import org.mapreduce.dumbo.ReducerListener;

public class WordCountReducer implements Reducer<String, String, Integer> {

	private final String word;
	private final List<String> filenames = new LinkedList<>();
	private final List<ReducerListener<String, String, Integer>> listeners = new ArrayList<ReducerListener<String, String, Integer>>();
	
	public WordCountReducer(String word) {
		this.word = word;
	}

	@Override
	public void merge(String value) {
		filenames.add(value);
	}

	@Override
	public void reduce(boolean async) {
		
		Runnable runnable = new Runnable() {

			@Override
			public void run() {
				Map<String, Integer> reducedMap = new HashMap<String, Integer>();

				for (String filename : filenames) {
					
					Integer count = reducedMap.get(filename);
					
					if (count == null) {
						reducedMap.put(filename, new Integer(1));
					} else {
						reducedMap.put(filename, new Integer(count.intValue() + 1));
					}
				}
				
				for(ReducerListener<String, String, Integer> listener: listeners) {
					listener.reduceDone(word, reducedMap);
				}				
			}
		};
		
		if (async) {
			Thread t = new Thread(runnable);
			t.start();
		} else {
			runnable.run();
		}
	}

	@Override
	public String getId() {
		return word;
	}

	@Override
	public void addListener(ReducerListener<String, String, Integer> listener) {
		if (!listeners.contains(listener)) listeners.add(listener);
	}
}