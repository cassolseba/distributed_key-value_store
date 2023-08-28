package it.unitn.ds1;

import java.util.HashMap;

import it.unitn.ds1.DataManager.Data;

import java.util.*;

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

    public HashMap<Integer, Data> getData() {
        return mostUpdatedData;
    }

}
