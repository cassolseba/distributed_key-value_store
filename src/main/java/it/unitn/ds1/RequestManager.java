package it.unitn.ds1;
import java.util.HashMap;

import akka.actor.*;
import it.unitn.ds1.DataManager.Data;

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
        private MRequestType type;
        //                    version, counter
        private final HashMap<Integer, Integer> counterMap;
        //                    version, value
        private final HashMap<Integer, String> valueMap;
        private String quoredValue;

        public RequestStruct(ActorRef client, String requestId, MRequestType type) {
            switch (type) {
                case READER -> this.quorumVal = R_quorum;
                case UPDATE -> this.quorumVal = W_quorum;
                default     -> this.quorumVal = W_quorum;
            }
            this.client = client;
            this.totalCounter = 0;
            this.type = type;
            this.counterMap = new HashMap<>();
            this.valueMap = new HashMap<>();
        }

        public String getQuoredValue() {
            return quoredValue;
        }

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
    }

    public enum RMresponse {
        NOTHING,
        OK,
        NOT_OK
    }

    public void create(String requestId, ActorRef client, MRequestType type) {
        requests.put(requestId, new RequestStruct(client, requestId, type));
    }

    public RMresponse add(String requestId, Data data) {
        RequestStruct state = requests.get(requestId);
        if (state == null)
            return RMresponse.NOTHING;
        if (state.update(data)) {
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


}
