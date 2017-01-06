package org.mapreduce;

public class MappedItem { 
     
    private final String word;
    private final String file;
     
    public MappedItem(String word, String file) {
        this.word = word;
        this.file = file;
    }
 
    public String getWord() {
        return word;
    }
 
    public String getFile() {
        return file;
    }
    
    @Override
    public String toString() {
    	return "(" + word + ", " + file + ")";
    }
}