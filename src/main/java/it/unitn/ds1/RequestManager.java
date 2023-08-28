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
    private final int writeQuorum;
    private final int readQuorum;

    //                 requestId, requestStatus
    private final HashMap<String, WriteReq> WriteReq;
    private final HashMap<String, ReadReq> ReadReq;

    public RequestManager(int writeQuorum, int readQuorum) {
        this.writeQuorum = writeQuorum;
        this.readQuorum = readQuorum;
        this.WriteReq = new HashMap<>();
        this.ReadReq = new HashMap<>();
    }

    private class ReadReq {
        private Integer totalCounter;
        private final int quorumVal;
        private final ActorRef client;
        //                    version, counter
        private final HashMap<Integer, Integer> counterMap;
        //                    version, value
        private final HashMap<Integer, String> valueMap;
        private String quoredValue;

        public ReadReq(ActorRef client) {
            quorumVal = readQuorum;
            this.client = client;
            this.totalCounter = 0;
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
            } else
                return false;
        }
    }

    private class WriteReq {
        private Integer totalCounter;
        private final int quorumVal;
        private final ActorRef client;
        //                    version, counter
        private final HashMap<Integer, Integer> counterMap;
        private Integer quoredVersion;
        private final String updateValue;
        private final Integer updateKey;

        public WriteReq(ActorRef client, Integer updateKey, String updateValue) {
            quorumVal = writeQuorum;
            this.client = client;
            this.totalCounter = 0;
            this.counterMap = new HashMap<>();
            this.updateKey = updateKey;
            this.updateValue = updateValue;
        }

        public Integer getQuoredVersion() {
            return quoredVersion;
        }

        public String getUpdateValue() {
            return updateValue;
        }

        public Integer getUpdateKey() {
            return updateKey;
        }

        public Boolean update(Integer version) {
            totalCounter++;
            counterMap.put(version, counterMap.getOrDefault(version, 0) + 1);

            if (counterMap.get(version) > quorumVal) {
                quoredVersion = version;
                return true;
            } else
                return false;
        }
    }

    public enum RequestManagerResp {
        NOTHING,
        OK,
        NOT_OK
    }

    /* ------- methods for read requests ------- */

    /**
     * Initialize a new read quorum
     *
     * @param requestId Identifier of the request
     * @param client    Reference to client node
     */
    public void newReadReq(String requestId, ActorRef client) {
        ReadReq.put(requestId, new ReadReq(client));
    }

    /**
     * Used to add a read message
     *
     * @param requestId Identifier if the request
     * @param data
     * @return the decision {OK, NOTHING}
     */
    public RequestManagerResp addReadReq(String requestId, Data data) {
        ReadReq state = ReadReq.get(requestId);
        if (state == null)
            return RequestManagerResp.NOTHING;
        if (state.update(data)) {
            return RequestManagerResp.OK;
        }
        return RequestManagerResp.NOTHING;
    }

    public Boolean isTimeoutOnRead(String requestId) {
        ReadReq state = ReadReq.get(requestId);
        return state != null;
    }

    public ActorRef getClientReadReq(String requestId) {
        return ReadReq.get(requestId).client;
    }

    public String getReadValue(String requestId) {
        return ReadReq.get(requestId).getQuoredValue();
    }

    public void removeReadReq(String requestId) {
        ReadReq.remove(requestId);
    }


    /* ------- methods for write requests ------- */

    /**
     * Initialize a new update quorum
     *
     * @param requestId   Identifier of the request
     * @param client      Reference to client node
     * @param updateKey   Key that identify data to update
     * @param updateValue New value to store for the specified key
     */
    public void newWriteReq(String requestId, ActorRef client, Integer updateKey, String updateValue) {
        WriteReq.put(requestId, new WriteReq(client, updateKey, updateValue));
    }

    // used in add an update message

    /**
     * Used to add an update message
     *
     * @param requestId Identifier of the request
     * @param version
     * @return the decision {OK, NOTHING}
     */
    public RequestManagerResp addWriteReq(String requestId, Integer version) {
        WriteReq state = WriteReq.get(requestId);
        if (state == null)
            return RequestManagerResp.NOTHING;
        if (state.update(version)) {
            return RequestManagerResp.OK;
        }
        return RequestManagerResp.NOTHING;
    }

    public Boolean isTimeoutOnWrite(String requestId) {
        WriteReq state = WriteReq.get(requestId);
        return state != null;
    }

    public ActorRef getClientWriteReq(String requestId) {
        return WriteReq.get(requestId).client;
    }

    public Integer getVersionOnWrite(String requestId) {
        return WriteReq.get(requestId).getQuoredVersion();
    }

    public void removeWriteReq(String requestId) {
        WriteReq.remove(requestId);
    }

    public String getNewValueOnWrite(String requestId) {
        return WriteReq.get(requestId).getUpdateValue();
    }

    public Integer getNewKeyOnWrite(String requestId) {
        return WriteReq.get(requestId).getUpdateKey();
    }
}
