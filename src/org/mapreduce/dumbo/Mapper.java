package org.mapreduce.dumbo;

public interface Mapper<U,I,K,V> {
	
	public U getId();
	
	public void map(I input, boolean async);
	
	public void addListener(MapperListener<U,K,V> listener);
	
	public boolean isDone();
}