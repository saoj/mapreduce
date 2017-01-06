package org.mapreduce;

import java.util.List;

public interface MapCallback {
     
    public void mapDone(String filename, List<MappedItem> values);
}