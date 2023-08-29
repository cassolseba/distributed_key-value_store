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
    private final HashMap<String, WriteReq> writeReq;
    private final HashMap<String, ReadReq> readReq;

    public RequestManager(int writeQuorum, int readQuorum) {
        this.writeQuorum = writeQuorum;
        this.readQuorum = readQuorum;
        this.writeReq = new HashMap<>();
        this.readReq = new HashMap<>();
    }

    /**
     * Class used to store information about a read request
     * It stores the responses from the nodes and keep in memory the most recent value according to the quorum
     */
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

        /**
         * Get the most recent value for the request according to the responses received
         * @return the most recent value
         */
        public String getQuoredValue() {
            return quoredValue;
        }

        /**
         * Update the quorum for a given read request
         * @param data Data received from a data node
         * @return true if the quorum is reached, false otherwise
         */
        public Boolean updateQuorum(Data data) {
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

    /**
     * Class used to store information about an update request
     * It stores the responses from the nodes and keep in memory the most recent version according to the quorum
     */
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

        /**
         * Get the most recent version for the request according to the responses received
         * @return the most recent version
         */
        public Integer getQuoredVersion() {
            return quoredVersion;
        }

        /**
         * Get the new value from the request
         * @return the new value
         */
        public String getUpdateValue() {
            return updateValue;
        }

        /**
         * Get the new key from the request
         * @return the new key
         */
        public Integer getUpdateKey() {
            return updateKey;
        }

        /**
         * Update the quorum for a given update request
         * @param version Version received from a data node
         * @return true if the quorum is reached, false otherwise
         */
        public Boolean updateQuorum(Integer version) {
            totalCounter++;
            counterMap.put(version, counterMap.getOrDefault(version, 0) + 1);

            if (counterMap.get(version) > quorumVal) {
                quoredVersion = version;
                return true;
            } else
                return false;
        }
    }

    /* ------- methods for read requests ------- */

    /**
     * Initialize a new read quorum
     * @param requestId Identifier of the request
     * @param client    Reference to client node
     */
    public void newReadReq(String requestId, ActorRef client) {
        readReq.put(requestId, new ReadReq(client));
    }

    /**
     * Update the quorum for a given read request
     * @param requestId Identifier of the request
     * @param data Data received from a data node
     * @return the decision {OK, NOTHING}
     */
    public RequestManagerResp addReadResp(String requestId, Data data) {
        ReadReq state = readReq.get(requestId);
        if (state == null)
            return RequestManagerResp.NOTHING;
        if (state.updateQuorum(data)) {
            return RequestManagerResp.OK;
        }
        return RequestManagerResp.NOTHING;
    }

    /**
     * Check if a read request is still active
     * @param requestId Identifier of the request
     * @return true if the request is still active, false otherwise
     */
    public Boolean isTimeoutOnRead(String requestId) {
        ReadReq state = readReq.get(requestId);
        return state != null;
    }

    /**
     * Get the client reference for a given request
     * @param requestId Identifier of the request
     * @return the client reference
     */
    public ActorRef getClientReadReq(String requestId) {
        return readReq.get(requestId).client;
    }

    /**
     * Get the most recent value for a given request
     * @param requestId Identifier of the request
     * @return the most recent value
     */
    public String getReadValue(String requestId) {
        return readReq.get(requestId).getQuoredValue();
    }

    /**
     * Remove a read request
     * @param requestId Identifier of the request to be removed
     */
    public void removeReadReq(String requestId) {
        readReq.remove(requestId);
    }


    /* ------- methods for write requests ------- */

    /**
     * Initialize a new update quorum
     * @param requestId   Identifier of the request
     * @param client      Reference to client node
     * @param updateKey   Key that identify data to update
     * @param updateValue New value to store for the specified key
     */
    public void newWriteReq(String requestId, ActorRef client, Integer updateKey, String updateValue) {
        writeReq.put(requestId, new WriteReq(client, updateKey, updateValue));
    }

    /**
     * Update the quorum for a given update request
     * @param requestId Identifier of the request
     * @param version Version received from a data node
     * @return the decision {OK, NOTHING}
     */
    public RequestManagerResp addWriteResp(String requestId, Integer version) {
        WriteReq state = writeReq.get(requestId);
        if (state == null)
            return RequestManagerResp.NOTHING;
        if (state.updateQuorum(version)) {
            return RequestManagerResp.OK;
        }
        return RequestManagerResp.NOTHING;
    }

    /**
     * Check if a write request is still active
     * @param requestId Identifier of the request
     * @return true if the request is still active, false otherwise
     */
    public Boolean isTimeoutOnWrite(String requestId) {
        WriteReq state = writeReq.get(requestId);
        return state != null;
    }

    /**
     * Get the client reference for a given request
     * @param requestId Identifier of the request
     * @return the client reference
     */
    public ActorRef getClientWriteReq(String requestId) {
        return writeReq.get(requestId).client;
    }

    /**
     * Get the most recent version for a given request
     * @param requestId Identifier of the request
     * @return the most recent version
     */
    public Integer getVersionOnWrite(String requestId) {
        return writeReq.get(requestId).getQuoredVersion();
    }

    /**
     * Remove a write request
     * @param requestId Identifier of the request to be removed
     */
    public void removeWriteReq(String requestId) {
        writeReq.remove(requestId);
    }

    /**
     * Get the new value from the request
     * @param requestId Identifier of the request
     * @return the new value
     */
    public String getNewValueOnWrite(String requestId) {
        return writeReq.get(requestId).getUpdateValue();
    }

    /**
     * Get the new key from the request
     * @param requestId Identifier of the request
     * @return the new key
     */
    public Integer getNewKeyOnWrite(String requestId) {
        return writeReq.get(requestId).getUpdateKey();
    }

    /**
     * Enum used to return the decision of the request manager
     */
    public enum RequestManagerResp {
        NOTHING,
        OK
    }
}
