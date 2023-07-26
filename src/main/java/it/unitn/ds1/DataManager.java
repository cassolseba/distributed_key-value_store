package it.unitn.ds1;
import java.util.*;

/**
 * DataManager
 * A class used to manage local data of data node.
 * Instantiated by every node.
 */
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
        public Data (Data data2) {
            this.value = data2.getValue(); this.version = data2.getVersion();
        }
        public String getValue() { return this.value; }
        public Integer getVersion() { return this.version; }
        public void update(String newValue) {
            value = newValue; version += 1;
        }
        public Boolean isNewer(Data data2) {
            return this.version > data2.getVersion();
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

    public void putData(Integer key, Data itemData) {
        storage.put(key, itemData);
    }


    public void putNewData(Integer key, Data itemData) {
        Data oldData = storage.get(key);
        if (oldData == null) {
            storage.put(key, itemData);
        }
    }

    public void putUpdate(Integer key, String value, Integer version) {
        storage.put(key, new Data(value, version));
    }

    public Data get(Integer key) {
        return storage.get(key);
    }

    public Set<Integer> getKeys() {
        return storage.keySet();
    }

    public String getValue(Integer key) {
        return storage.get(key).getValue();
    }

    public void remove(Integer key) {
        System.out.println("[]" + getValue(key));
        storage.remove(key);
    }

    public Map<Integer, Data> getAllData() {
        return storage;
    }
}
