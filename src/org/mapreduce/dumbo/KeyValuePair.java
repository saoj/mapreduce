package org.mapreduce.dumbo;

public class KeyValuePair<K, V> {
	
    public final K key; 
    public final V value;
    
    public KeyValuePair(K key, V value) {
        this.key = key;
        this.value = value;
    }

    @Override
    public String toString() {
        return "(" + key + "," + value + ")";
    }

    @SuppressWarnings("unchecked")
	@Override
    public boolean equals(Object o) {
    	if (o instanceof KeyValuePair) {
    		KeyValuePair<K,V> other = (KeyValuePair<K,V>) o;
    		return compare(this.key, other.key) && compare(this.value, other.value);
    	}
    	return false;
    }
    
    private boolean compare(Object o1, Object o2) {
    	if (o1 == null) {
    		if (o2 == null) {
    			return true;
    		} else {
    			return false;
    		}
    	} else {
    		return o1.equals(o2);
    	}
    }

    @Override
    public int hashCode() {
        int result = 1;
        result = 31 * result + ((key == null) ? 0 : key.hashCode());
        result = 31 * result + ((value == null) ? 0 : value.hashCode());
        return result;
    }
}