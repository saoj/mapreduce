package org.mapreduce.dumbo.wordcount;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.mapreduce.dumbo.KeyValuePair;
import org.mapreduce.dumbo.MapperListener;
import org.mapreduce.dumbo.ReducerListener;

public class WithMapReduceAndParallelization {
	
	public static void main(String[] args) {
		
		final List<FileMapper> mappers = new ArrayList<FileMapper>();
		final Map<String, WordCountReducer> reducers = new HashMap<String, WordCountReducer>();
		
		final ReducerListener<String, String, Integer> reducerListener = new ReducerListener<String, String, Integer>() {
			@Override
			public synchronized void reduceDone(String id, Map<String, Integer> reducedMap) {
				System.out.println(id + " => " + reducedMap);
			}
		};
		
		final MapperListener<String, String, String> mapperListener = new MapperListener<String, String, String>() {
			@Override
			public synchronized void mapDone(String id, List<KeyValuePair<String, String>> mappedItems) {
				
				for(KeyValuePair<String, String> keyValuePair: mappedItems) {
					String word = keyValuePair.key;
					String filename = keyValuePair.value;
					
					WordCountReducer reducer = reducers.get(word);
					
					if (reducer == null) {
						reducer = new WordCountReducer(word);
						reducer.addListener(reducerListener);
						reducers.put(word, reducer);
					}
					
					reducer.merge(filename);
				}
			}
		};
		
		// MAP:
		
		mappers.add(launchMapper("file1.txt", "foo foo bar cat dog dog", mapperListener));
		mappers.add(launchMapper("file2.txt", "foo house cat cat dog", mapperListener));
		mappers.add(launchMapper("file3.txt", "foo bird foo foo", mapperListener));
		
		waitForMappersToFinish(mappers);
		
		// REDUCE:
		
		for(WordCountReducer reducer: reducers.values()) {
			reducer.reduce(true);
		}
	}
	
	private static FileMapper launchMapper(String filename, String contents, MapperListener<String, String, String> listener) {
		FileMapper mapper = new FileMapper(filename);
		mapper.addListener(listener);
		mapper.map(contents, true);
		return mapper;
	}
	
	private static void waitForMappersToFinish(List<FileMapper> mappers) {

		while(!areMappersDone(mappers)) {
			try {
				Thread.sleep(1);
			} catch(Exception e) {
				throw new RuntimeException(e);
			}
		}
	}
	
	private static boolean areMappersDone(List<FileMapper> mappers) {
		for(FileMapper mapper: mappers) {
			if (!mapper.isDone()) return false;
		}
		return true;
	}
	
}