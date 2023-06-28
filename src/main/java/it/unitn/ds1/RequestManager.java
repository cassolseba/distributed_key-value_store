package it.unitn.ds1;
import java.util.HashMap;

import akka.actor.*;
import it.unitn.ds1.DataManager.Data;
import scala.Int;

import java.util.*;

public class RequestManager {
    private final int W_quorum;
    private final int R_quorum;

    private final HashMap<String, RequestStruct> requests;

    public RequestManager(int W_quorum, int R_quorum) {
       this.W_quorum = W_quorum;
       this.R_quorum = R_quorum;
       this.requests = new HashMap<>();
    }

    public enum MRequestType {
        READER,
        UPDATE
    }

    private class RequestStruct {
        private Integer totalCounter;
        private final int quorumVal;
        private final ActorRef client;
        //                    version, counter
        private final HashMap<Integer, Integer> counterMap;
        //                    version, value
        private final HashMap<Integer, String> valueMap;
        private String quoredValue;

        private Integer quoredVersion;
        private String updateValue;
        private Integer updateKey;

        public RequestStruct(ActorRef client, String requestId, MRequestType type) {
            switch (type) {
                case READER -> this.quorumVal = R_quorum;
                case UPDATE -> this.quorumVal = W_quorum;
                default     -> this.quorumVal = W_quorum;
            }
            this.client = client;
            this.totalCounter = 0;
            this.counterMap = new HashMap<>();
            this.valueMap = new HashMap<>();
        }

        public String getQuoredValue() { return quoredValue; }
        public Integer getQuoredVersion() { return quoredVersion; }
        public String getUpdateValue() { return updateValue; }
        public Integer getUpdateKey() { return updateKey; }

        // used in reading
        public Boolean update(Data data) {
            totalCounter++;
            valueMap.put(data.getVersion(), data.getValue());
            counterMap.put(data.getVersion(), counterMap.getOrDefault(data.getVersion(), 0) + 1);

           if (counterMap.get(data.getVersion()) > quorumVal) {
                quoredValue = valueMap.get(data.getVersion());
                return true;
           }
           else
               return false;
        }

        // used in updating
        public Boolean update(Integer version) {
            totalCounter++;
            counterMap.put(version, counterMap.getOrDefault(version, 0) + 1);

           if (counterMap.get(version) > quorumVal) {
                quoredVersion = version;
                return true;
           }
           else
               return false;
        }
    }

    public enum RMresponse {
        NOTHING,
        OK,
        NOT_OK
    }

    public void create(String requestId, ActorRef client, MRequestType type) {
        requests.put(requestId, new RequestStruct(client, requestId, type));
    }

    // used in reading the data
    public RMresponse add(String requestId, Data data) {
        RequestStruct state = requests.get(requestId);
        if (state == null)
            return RMresponse.NOTHING;
        if (state.update(data)) {
            return RMresponse.OK;
        }
        return RMresponse.NOTHING;
    }

    // used in updating the data
    public void addUpdate(String requestId, String value, Integer key) {
        RequestStruct state = requests.get(requestId);
        if (state == null)
            return;
        state.updateValue = value;
        state.updateKey = key;
    }

    public RMresponse add(String requestId, Integer version) {
        RequestStruct state = requests.get(requestId);
        if (state == null)
            return RMresponse.NOTHING;
        if (state.update(version)) {
            return RMresponse.OK;
        }
        return RMresponse.NOTHING;
    }

    public ActorRef getActorRef(String requestId) {
        return requests.get(requestId).client;
    }

    public String getValueAndRemove(String requestId) {
        String value = requests.get(requestId).getQuoredValue();
        requests.remove(requestId);
        return value;
    }

    public Integer getVersionAndRemove(String requestId) {
        Integer version = requests.get(requestId).getQuoredVersion();
        requests.remove(requestId);
        return version;
    }

    public String getUpdateValue(String requestId) {
        String update = requests.get(requestId).getUpdateValue();
        return update;
    }

    public Integer getUpdateKey(String requestId) {
        Integer update = requests.get(requestId).getUpdateKey();
        return update;
    }
}
