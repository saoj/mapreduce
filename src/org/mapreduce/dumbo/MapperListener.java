package org.mapreduce.dumbo;

import java.util.List;

public interface MapperListener<U,K,V> {
	
	public void mapDone(U id, List<KeyValuePair<K,V>> mappedItems);
}