package it.unitn.ds1.managers;

import java.util.HashMap;

import it.unitn.ds1.managers.DataManager.Data;

import java.util.*;

/**
 * JoinManager
 * This class is used to manage the join operation of a new node.
 */
public class JoinManager {
    private final int replicasCount;

    private final HashMap<Integer, Data> mostUpdatedData;
    private final HashMap<Integer, Integer> counter;
    private final Set<Integer> items;

    public JoinManager(int replicasCount, Set<Integer> items) {
        this.replicasCount = replicasCount;
        this.items = new HashSet<>(items);
        this.counter = new HashMap<>();
        this.mostUpdatedData = new HashMap<>();
    }

    /**
     * Retrieve the data for a given key and add it to the mostUpdatedData map.
     * If the data is already present, check if the new data is newer and replace it.
     * @param key the key of the data
     * @param receivedData the data to add
     * @return true if all the data has been received, false otherwise
     */
    public Boolean addData(Integer key, Data receivedData) {
        Data currentData = mostUpdatedData.get(key);
        if (currentData == null) {
            mostUpdatedData.put(key, receivedData);
        } else {
            if (receivedData.isNewer(mostUpdatedData.get(key))) {
                mostUpdatedData.put(key, receivedData);
            }
        }
        counter.put(key, counter.getOrDefault(key, 0) + 1);

        if (counter.getOrDefault(key, 0) == replicasCount) {
            items.remove(key);
        }

        return items.isEmpty();
    }

    /**
     * Get the data that has been received.
     * @return the data that has been received
     */
    public HashMap<Integer, Data> getData() {
        return mostUpdatedData;
    }

}
