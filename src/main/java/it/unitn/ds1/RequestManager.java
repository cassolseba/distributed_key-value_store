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
        private final int quorumVal;
        private int currentVal;
        private final ActorRef client;
        private final String requestId;
        private MRequestType type;
        private final List<Data> values;
        public String test;

        public RequestStruct(ActorRef client, String requestId, MRequestType type) {
            switch (type) {
                case READER -> this.quorumVal = R_quorum;
                case UPDATE -> this.quorumVal = W_quorum;
                default     -> this.quorumVal = W_quorum;
            }
            this.client = client;
            this.requestId = requestId;
            this.type = type;
            this.currentVal = 0;
            this.values = new ArrayList<>();
        }

        public Boolean update(String value) {
           currentVal += 1;
           test = value;
           // System.out.println("currentVal: " + currentVal);
           if (currentVal > quorumVal)
                return true;
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

    public RMresponse add(String requestId, String value) {
        RequestStruct state = requests.get(requestId);
        if (state.update(value)) {
            return RMresponse.OK;
        }
        return RMresponse.NOTHING;
    }

    public ActorRef getActorRef(String requestId) {
        return requests.get(requestId).client;
    }

    public String getValue(String requestId) {
        return requests.get(requestId).test;
    }


}
