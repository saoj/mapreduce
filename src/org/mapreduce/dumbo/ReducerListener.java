package org.mapreduce.dumbo;

import java.util.Map;

public interface ReducerListener<U,V,R> {
    
    public void reduceDone(U id, Map<V, R> reducedMap);
    
}