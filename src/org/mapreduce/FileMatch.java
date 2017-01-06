package org.mapreduce;

public class FileMatch {
	
	private final String filename;
	private int occurrences;
	
	public FileMatch(String filename) {
		this.filename = filename;
		this.occurrences = 0;
	}
	
	public void inc() {
		this.occurrences++;
	}
	
	public String getFilename() {
		return filename;
	}
	
	public int getOccurrences() {
		return occurrences;
	}
	
	@Override
	public String toString() {
		return "(" + filename + ", " + occurrences + ")";
	}
}	
