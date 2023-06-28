package it.unitn.ds1;
import java.util.*;


public class DataManager {
    private final Map<Integer, Data> storage; // key - (value - version)

    public DataManager () {
        this.storage = new HashMap<>();
    }

    static public class Data {
        private String value;
        private Integer version;

        public Data (String value) {
            this.value = value; this.version = 1;
        }
        public Data (String value, Integer version) {
            this.value = value; this.version = version;
        }
        public String getValue() { return this.value; }
        public Integer getVersion() { return this.version; }
        public void update(String newValue) {
            value = newValue; version += 1;
        }
    }

    public void put(Integer key, String value) {
        Data oldData = storage.get(key);
        if (oldData == null) {
            storage.put(key, new Data(value));
        } else {
            oldData.update(value);
        }
    }

    public void putUpdate(Integer key, String value, Integer version) {
        storage.put(key, new Data(value, version));
    }

    public Data get(Integer key) {
        return storage.get(key);
    }

    public String getValue(Integer key) {
        return storage.get(key).getValue();
    }
}
