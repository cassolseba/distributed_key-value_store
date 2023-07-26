package it.unitn.ds1;
import java.util.HashMap;

import akka.actor.*;
import it.unitn.ds1.DataManager.Data;

/**
 * RequestManager
 * A class used to manage client requests received by the coordinators.
 * Instantiated by every data node.
 * Stores information about the senders of the request
 * Manages datanode responses and quorums of each ongoing client request as read, update.
 */
public class RequestManager {
    private final int W_quorum;
    private final int R_quorum;

    //                 requestId, requestStatus
    private final HashMap<String, Wrequest> Wrequests;
    private final HashMap<String, Rrequest> Rrequests;

    public RequestManager(int W_quorum, int R_quorum) {
       this.W_quorum = W_quorum;
       this.R_quorum = R_quorum;
       this.Wrequests = new HashMap<>();
       this.Rrequests = new HashMap<>();
    }

    private class Rrequest {
        private Integer totalCounter;
        private final int quorumVal;
        private final ActorRef client;
        //                    version, counter
        private final HashMap<Integer, Integer> counterMap;
        //                    version, value
        private final HashMap<Integer, String> valueMap;
        private String quoredValue;

        public Rrequest(ActorRef client) {
            quorumVal = R_quorum;
            this.client = client;
            this.totalCounter = 0;
            this.counterMap = new HashMap<>();
            this.valueMap = new HashMap<>();
        }

        public String getQuoredValue() { return quoredValue; }

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

    private class Wrequest {
        private Integer totalCounter;
        private final int quorumVal;
        private final ActorRef client;
        //                    version, counter
        private final HashMap<Integer, Integer> counterMap;
        private Integer quoredVersion;
        private final String updateValue;
        private final Integer updateKey;

        public Wrequest(ActorRef client, Integer updateKey, String updateValue) {
            quorumVal = W_quorum;
            this.client = client;
            this.totalCounter = 0;
            this.counterMap = new HashMap<>();
            this.updateKey = updateKey;
            this.updateValue = updateValue;
        }

        public Integer getQuoredVersion() { return quoredVersion; }
        public String getUpdateValue() { return updateValue; }
        public Integer getUpdateKey() { return updateKey; }

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

    ////////////////////////////
    // methods for read requests
    ////////////////////////////

    /**
     * Initialize a new read quorum
     * @param requestId Identifier of the request
     * @param client Reference to client node
     */
    public void createR(String requestId, ActorRef client) {
        Rrequests.put(requestId, new Rrequest(client));
    }

    /**
     * Used to add a read message
     * @param requestId Identifier if the request
     * @param data
     * @return the decision {OK, NOTHING}
     */
    public RMresponse addR(String requestId, Data data) {
        Rrequest state = Rrequests.get(requestId);
        if (state == null)
            return RMresponse.NOTHING;
        if (state.update(data)) {
            return RMresponse.OK;
        }
        return RMresponse.NOTHING;
    }

    public Boolean receiveTimeoutR(String requestId) {
        Rrequest state = Rrequests.get(requestId);
        return state != null;
    }

    public ActorRef getActorRefR(String requestId) {
        return Rrequests.get(requestId).client;
    }

    public String getValueR(String requestId) {
        return Rrequests.get(requestId).getQuoredValue();
    }

    public void removeR(String requestId) {
        Rrequests.remove(requestId);
    }

    /////////////////////////////
    // methods for write requests
    /////////////////////////////

    // initialize a new update quorum

    /**
     * Initialize a new update quorum
     * @param requestId Identifier of the request
     * @param client Reference to client node
     * @param updateKey Key that identify data to update
     * @param updateValue New value to store for the specified key
     */
    public void createW(String requestId, ActorRef client, Integer updateKey, String updateValue) {
        Wrequests.put(requestId, new Wrequest(client, updateKey, updateValue));
    }

    // used in add a update message

    /**
     * Used to add an update message
     * @param requestId Identifier of the request
     * @param version
     * @return the decision {OK, NOTHING}
     */
    public RMresponse addW(String requestId, Integer version) {
        Wrequest state = Wrequests.get(requestId);
        if (state == null)
            return RMresponse.NOTHING;
        if (state.update(version)) {
            return RMresponse.OK;
        }
        return RMresponse.NOTHING;
    }

    public Boolean receiveTimeoutW(String requestId) {
        Wrequest state = Wrequests.get(requestId);
        return state != null;
    }

    public ActorRef getActorRefW(String requestId) {
        return Wrequests.get(requestId).client;
    }

    public Integer getVersionW(String requestId) {
        return Wrequests.get(requestId).getQuoredVersion();
    }

    public void removeW(String requestId) {
        Wrequests.remove(requestId);
    }

    public String getUpdateValueW(String requestId) {
        return Wrequests.get(requestId).getUpdateValue();
    }

    public Integer getUpdateKeyW(String requestId) {
        return Wrequests.get(requestId).getUpdateKey();
    }
}
