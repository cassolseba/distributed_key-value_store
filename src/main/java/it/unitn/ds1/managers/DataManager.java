package it.unitn.ds1.managers;

import java.util.*;

/**
 * DataManager
 * A class used to manage local data of data node.
 * Instantiated by every node.
 */
public class DataManager {
    private final Map<Integer, Data> storage; // key - (value - version)
    private final Map<Integer, Boolean> blocks;

    public DataManager() {
        this.storage = new HashMap<>();
        this.blocks  = new HashMap<>();
    }

    /**
     * Data
     * A class that represent a data item.
     * It contains the value and the version of the data item.
     */
    static public class Data {
        private String value;
        private Integer version;

        public Data(String value) {
            this.value = value;
            this.version = 1;
        }

        public Data(String value, Integer version) {
            this.value = value;
            this.version = version;
        }

        /**
         * Get the value of the data item.
         * @return value
         */
        public String getValue() {
            return this.value;
        }

        /**
         * Get the version of the data item.
         * @return version
         */
        public Integer getVersion() {
            return this.version;
        }

        /**
         * Update the value of the data item.
         * @param newValue the new value of the data item
         */
        public void updateValue(String newValue) {
            value = newValue;
            version += 1;
        }

        /**
         * Check if the data item is newer than the given data item.
         * @param data the given data item
         * @return true if the data item is newer, false otherwise
         */
        public Boolean isNewer(Data data) {
            return this.version > data.getVersion();
        }
    }

    /**
     * Put a new data item into the storage.
     * @param key the key of the data item
     * @param value the value of the data item
     */
    public void put(Integer key, String value) {
        Data oldData = storage.get(key);
        if (oldData == null) {
            storage.put(key, new Data(value));
        } else {
            oldData.updateValue(value);
        }
        blocks.put(key, false);
    }

    /**
     * Put a new data item into the storage.
     * @param key the key of the data item
     * @param itemData the data item
     */
    public void putData(Integer key, Data itemData) {
        storage.put(key, itemData);
        blocks.put(key, false);
    }

    /**
     * Put a new data item into the storage.
     * Only if the data item is actually absent.
     * @param key the key of the data item
     * @param itemData the data item
     */
    public void putNewData(Integer key, Data itemData) {
        storage.putIfAbsent(key, itemData);
        blocks.put(key, false);
    }

    /**
     * Put an updated data item into the storage.
     * @param key the key of the data item
     * @param value the value of the data item
     * @param version the version of the data item
     */
    public void putUpdateAndRemoveBlock(Integer key, String value, Integer version) {
        storage.put(key, new Data(value, version));
        blocks.put(key, false);
    }

    /**
     * Put a set of new data item into the storage.
     * @param newData the set of new data item
     */
    public void add(Map<Integer, Data> newData) {
        for (Map.Entry<Integer, Data> entry : newData.entrySet()) {
            storage.merge(entry.getKey(), entry.getValue(),
                    (oldValue, newValue) -> newValue.isNewer(oldValue)
                            ? newValue
                            : oldValue);
            blocks.putIfAbsent(entry.getKey(), false);
        }
    }

    /**
     * Get the data item with the given key.
     * @param key the key of the data item
     * @return the data item
     */
    public Data getDataAndBlock(Integer key) {
        if (blocks.get(key) != null) {
            blocks.put(key, true);
        }
        return storage.get(key);
    }

    public void removeBlock(Integer key) {
        blocks.put(key, false);
    }

    public Data getData(Integer key) {
//        if (blocks.get(key)) {
//            return null;
//        }
        return storage.get(key);
    }

    /**
     * Get the set of keys of the data items.
     * @return the set of keys
     */
    public Set<Integer> getKeys() {
        return storage.keySet();
    }

    /**
     * Get the value of the data item with the given key.
     * @param key the key of the data item
     * @return the value of the data item
     */
    public String getValue(Integer key) {
        if (blocks.get(key)) {
            return null;
        }
        return storage.get(key).getValue();
    }

    /**
     * Remove the data item with the given key from the storage.
     * @param key the key of the data item
     */
    public void removeData(Integer key) {
        System.out.println("[]" + getValue(key));
        storage.remove(key);
        blocks.remove(key);
    }

    /**
     * Get all data items in the storage (i.e., the storage).
     * @return the storage
     */
    public Map<Integer, Data> getAllData() {
        return storage;
    }

    public Boolean isPresent(Integer key) {
        return storage.containsKey(key);
    }

    public Boolean isBlocked(Integer key) {
        return blocks.get(key);
    }
}
