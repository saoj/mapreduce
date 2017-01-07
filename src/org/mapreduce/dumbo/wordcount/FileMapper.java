package org.mapreduce.dumbo.wordcount;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.mapreduce.dumbo.KeyValuePair;
import org.mapreduce.dumbo.Mapper;
import org.mapreduce.dumbo.MapperListener;

public class FileMapper implements Mapper<String, String, String, String> {

	private final String filename;
	private final List<MapperListener<String, String, String>> listeners = new ArrayList<MapperListener<String, String, String>>();
	private volatile boolean isDone = false;

	public FileMapper(String filename) {
		this.filename = filename;
	}

	@Override
	public void map(String input, boolean async) {
		
		Runnable runnable = new Runnable() {
			
			public void run() {

				List<KeyValuePair<String, String>> mappedItems = new LinkedList<KeyValuePair<String, String>>();
		
				String[] words = input.split("\\s+");
		
				for (String word : words) {
					mappedItems.add(new KeyValuePair<>(word, filename));
				}
				
				for(MapperListener<String, String, String> listener: listeners) {
					listener.mapDone(filename, mappedItems);
				}
				
				isDone = true;
			
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
		return filename;
	}

	@Override
	public void addListener(MapperListener<String, String, String> listener) {
		if (!listeners.contains(listener)) listeners.add(listener);
	}
	
	@Override
	public final boolean isDone() {
		return isDone;
	}

}