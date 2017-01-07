package org.mapreduce.dumbo;

public interface Reducer<U,V,R> {
	
	public U getId();
	
	public void merge(V value);
	
	public void reduce(boolean async);
	
	public void addListener(ReducerListener<U,V,R> listener);
}