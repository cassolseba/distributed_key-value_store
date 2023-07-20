package it.unitn.ds1;
import java.util.HashMap;

import akka.actor.*;
import it.unitn.ds1.DataManager.Data;
import java.util.*;

public class JoinManager {
    private final int N_replica;

    private final HashMap<Integer, Data> mostUpdatedData;
    private final HashMap<Integer, Integer> counter;
    private final Set<Integer> items;

    public JoinManager(int N_replica, Set<Integer> items) {
        this.N_replica = N_replica;
        this.items = new HashSet<Integer>(items);
        this.counter = new HashMap<>();
        this.mostUpdatedData = new HashMap<>();
    }

    public Boolean add(Integer key, Data recvData) {
        Data curr = mostUpdatedData.get(key);
        if (curr == null) {
            mostUpdatedData.put(key, recvData);
        }
        else {
            if (recvData.isNewer(mostUpdatedData.get(key))) {
                mostUpdatedData.put(key, recvData);
            }
        }
        counter.put(key, counter.getOrDefault(key, 0) + 1);

        if (counter.getOrDefault(key, 0) == N_replica) {
            items.remove(key);
        }

        if (items.size() == 0) {
            return true;
        }
        return false;
    }

    public HashMap<Integer, Data> getData() {
        return mostUpdatedData;
    }

}
