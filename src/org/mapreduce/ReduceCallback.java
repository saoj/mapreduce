package org.mapreduce;

import java.util.Map;

public interface ReduceCallback {
    
    public void reduceDone(String word, Map<String, FileMatch> reducedMap);
}