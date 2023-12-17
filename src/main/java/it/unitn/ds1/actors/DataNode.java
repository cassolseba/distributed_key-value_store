package it.unitn.ds1.actors;

import akka.actor.*;
import it.unitn.ds1.logger.ErrorType;
import it.unitn.ds1.managers.DataManager;
import it.unitn.ds1.managers.GroupManager;
import it.unitn.ds1.managers.GroupManager.DataNodeRef;
import it.unitn.ds1.managers.JoinManager;
import it.unitn.ds1.managers.RequestManager;
import it.unitn.ds1.logger.Logs;
import it.unitn.ds1.logger.TimeoutType;
import it.unitn.ds1.utils.Helper;
import scala.concurrent.duration.Duration;

import java.util.concurrent.TimeUnit;

import it.unitn.ds1.managers.DataManager.Data;

import java.util.stream.Collectors;

import java.io.Serializable;
import java.util.*;

/**
 * DataNode
 * Actor that represents a data node in the distributed database
 */
public class DataNode extends AbstractActor {
    private final int maxTimeout; // in ms
    public final Integer nodeKey; // Node key
    private final DataManager nodeData;
    private final GroupManager groupManager;
    private final RequestManager requestManager;
    private JoinManager joinManager;

    public DataNode(int writeQuorum, int readQuorum, int replicas, int maxTimeout, int nodeKey) {
        this.maxTimeout = maxTimeout;
        this.nodeKey = nodeKey;
        this.requestManager = new RequestManager(writeQuorum, readQuorum);
        this.nodeData = new DataManager();
        this.groupManager = new GroupManager(replicas);

        // Logging
        System.out.println("INIT_NODE | Name: " + Helper.getName(self()) + ", key: " + nodeKey + " |");
    }

    static public Props props(int writeQuorum, int readQuorum, int replicas, int maxTimeout, int nodeKey) {
        return Props.create(DataNode.class, () -> new DataNode(writeQuorum, readQuorum, replicas, maxTimeout, nodeKey));
    }

    /**
     * Make the data node switch context to crash behavior.
     */
    private void crash() {
        getContext().become(crashed());
    }

    /**
     * Make the data node switch to the normal behavior i.e. recovering
     */
    private void recover() {
        getContext().become(createReceive());
    }

    /**
     * Drop the items that this node is not responsible anymore
     */
    private void dropUselessItems() {
        nodeData.getKeys().removeIf(item -> !groupManager.findDataNodes(item).contains(self()));
    }

    /* ------- MESSAGES ------- */

    /**
     * InitializaDataGroup
     * A message that initializes the group of nodes in this node.
     */
    public static class InitializeDataGroup implements Serializable {
        public final List<DataNodeRef> group;

        public InitializeDataGroup(List<DataNodeRef> group) {
            this.group = Collections.unmodifiableList(new ArrayList<>(group));
        }
    }

    /* ------- WRITE ------- */

    /**
     * AskWriteData
     * A message that starts the write operation in the database.
     * It is sent by the client and received by the coordinator data node.
     */
    public static class AskWriteData implements Serializable {
        public final Integer key;
        public final String value;

        /**
         * @param key the key to write
         * @param value the value to write
         */
        public AskWriteData(Integer key, String value) {
            this.key = key;
            this.value = value;
        }
    }

    /**
     * WriteData
     * A message that tells the datanode to write the data.
     * It is sent by the coordinator and received by the proper datanode.
     */
    public static class WriteData implements Serializable {
        public final Integer key;
        public final String value;

        /**
         * @param key the key to write
         * @param value the value to write
         */
        public WriteData(Integer key, String value) {
            this.key = key;
            this.value = value;
        }
    }

    /* ------- READ ------- */

    /**
     * AskReadData
     * A message that starts the read operation in the database.
     * It is sent by the client and received by the coordinator data node.
     */
    public static class AskReadData implements Serializable {
        public final Integer key;
        public final String requestId;

        /**
         * @param key the key to read
         * @param requestId the request identifier
         */
        public AskReadData(Integer key, String requestId) {
            this.key = key;
            this.requestId = requestId;
        }
    }

    /**
     * ReadData
     * A message that tells the datanode to read the data.
     * It is sent by the coordinator and received by the proper datanode.
     */
    public static class ReadData implements Serializable {
        public final Integer key;
        public final String requestId;

        /**
         * @param key the key to read
         * @param requestId the request identifier
         */
        public ReadData(Integer key, String requestId) {
            this.key = key;
            this.requestId = requestId;
        }
    }

    /**
     * TimeoutOnRead
     * A message that returns a timeout during a read operation.
     * It is sent by the data node and received by the coordinator.
     */
    public static class TimeoutOnRead implements Serializable {
        public final String requestId;

        /**
         * @param requestId the request identifier
         */
        public TimeoutOnRead(String requestId) {
            this.requestId = requestId;
        }
    }

    /**
     * ReturnTimeoutOnRead
     * A message that tells the client that a timeout occurred during the read operation.
     * It is sent by the coordinator and received by the client.
     */
    public static class ReturnTimeoutOnRead implements Serializable {
        public final String requestId;

        /**
         * @param requestId the request identifier
         */
        public ReturnTimeoutOnRead(String requestId) {
            this.requestId = requestId;
        }
    }

    /**
     * SendRead
     * A message that returns the value of the requested key.
     * It is sent by the data node and received by the coordinator.
     */
    public static class SendRead implements Serializable {
        public final Data data;
        public final String requestId;

        /**
         * @param data the requested data
         * @param requestId the request identifier
         */
        public SendRead(Data data, String requestId) {
            this.data = data;
            this.requestId = requestId;
        }
    }

    /**
     * SendRead2Client
     * A message that forwards the result of the read to the client.
     * It is sent by the coordinator and received by the client.
     */
    public static class SendRead2Client implements Serializable {
        public final String value;
        public final String requestId;

        /**
         * @param value the value of the requested key
         * @param requestId the request identifier
         */
        public SendRead2Client(String value, String requestId) {
            this.value = value;
            this.requestId = requestId;
        }
    }

    /* ------- UPDATE ------- */

    /**
     * AskUpdateData
     * A message that starts the update operation in the data nodes.
     * It is sent by the client and received by the coordinator.
     */
    public static class AskUpdateData implements Serializable {
        public final Integer key;
        public final String value;
        public final String requestId;

        /**
         * @param key the key to update
         * @param value the new value
         * @param requestId the request identifier
         */
        public AskUpdateData(Integer key, String value, String requestId) {
            this.key = key;
            this.value = value;
            this.requestId = requestId;
        }
    }

    /**
     * AskVersion
     * A message used to ask the datanode the version associated to the key.
     * It is sent by the coordinator and received by the proper datanode.
     */
    public static class AskVersion implements Serializable {
        public final Integer key;
        public final String requestId;

        /**
         * @param key the key to update
         * @param requestId the request identifier
         */
        public AskVersion(Integer key, String requestId) {
            this.key = key;
            this.requestId = requestId;
        }
    }

    /**
     * SendVersion
     * A message used to return the version of the data to update in this node.
     * It is sent by the data nodes and received by the coordinator.
     */
    public static class SendVersion implements Serializable {
        public final Integer version;
        public final String requestId;

        /**
         * @param version the actual version
         * @param requestId the request identifier
         */
        public SendVersion(Integer version, String requestId) {
            this.version = version;
            this.requestId = requestId;
        }
    }

    /**
     * TimeoutOnUpdate
     * A message that returns a timeout during an update operation.
     * It is sent by the data node and received by the coordinator.
     */
    public static class TimeoutOnUpdate implements Serializable {
        public final String requestId;

        /**
         * @param requestId the request identifier
         */
        public TimeoutOnUpdate(String requestId) {
            this.requestId = requestId;
        }
    }

    /**
     * ReturnTimeoutOnWrite
     * A message that tells the client that a timeout occurred during an update operation.
     * It is sent by the coordinator and received by the client.
     */
    public static class ReturnTimeoutOnWrite implements Serializable {
        public final String requestId;

        /**
         * @param requestId the request identifier
         */
        public ReturnTimeoutOnWrite(String requestId) {
            this.requestId = requestId;
        }
    }

    /**
     * ReturnUpdate
     * A message that tells the result of the update operation to the coordinator.
     * It is sent by the coordinator and received by the client.
     */
    public static class ReturnUpdate implements Serializable {
        public final Integer version;
        public final String requestId;

        /**
         * @param version the actual version
         * @param requestId the request identifier
         */
        public ReturnUpdate(Integer version, String requestId) {
            this.version = version;
            this.requestId = requestId;
        }
    }

    /**
     * UpdateData
     * A message that tells the data node to perform the update.
     * It is sent by the coordinator and received by the proper data node.
     */
    public static class UpdateData implements Serializable {
        public final Integer key;
        public final String value;
        public final Integer version;

        /**
         * @param key the key to update
         * @param value the new value
         * @param version the new version
         */
        public UpdateData(Integer key, String value, Integer version) {
            this.key = key;
            this.value = value;
            this.version = version;
        }
    }

    /* ------- JOIN ------- */

    /**
     * AskToJoin
     * A message that starts the join operation.
     * It is sent by the system and received by the joining node
     */
    public static class AskToJoin implements Serializable {
        public ActorRef bootstrappingNode;

        /**
         * @param node the ActorRef of the boostrap node
         */
        public AskToJoin(ActorRef node) {
            this.bootstrappingNode = node;
        }
    }

    /**
     * AskNodeGroup
     * A message that ask for the actual group of nodes to the boostrap node.
     * It is sent by the joining data node and received by the boostrap node.
     */
    public static class AskNodeGroup implements Serializable {
        public AskNodeGroup() {
        }
    }

    /**
     * SendNodeGroup
     * A message that returns the group of nodes to the joining data node.
     * It is sent by the bootstrap node and received by the joining data node.
     */
    public static class SendNodeGroup implements Serializable {
        public final List<DataNodeRef> group;

        /**
         * @param group the list of ActorRef that form the group
         */
        public SendNodeGroup(List<DataNodeRef> group) {
            this.group = Collections.unmodifiableList(new ArrayList<>(group));
        }
    }

    /**
     * AskItems
     * A message that requests the set of keys.
     * It is sent by the joining node and received by its neighbors.
     */
    public static class AskItems implements Serializable {
        public AskItems() {
        }
    }

    /**
     * A message that returns the set of the keys of the node.
     * It is received by the joining node and sent by one of its neighbors.
     */
    public static class SendItems implements Serializable {
        public final Set<Integer> keys;

        /**
         * @param keys the set of keys of the neighbor
         */
        public SendItems(Set<Integer> keys) {
            this.keys = Collections.unmodifiableSet(new HashSet<>(keys));
        }
    }

    /**
     * AskItemData
     * A message that requests the value of a key.
     * It is sent by the joining data node and received by the neighbors.
     */
    public static class AskItemData implements Serializable {
        public Integer key;

        /**
         * @param key the requested key
         */
        public AskItemData(Integer key) {
            this.key = key;
        }
    }

    /**
     * SendItemData
     * A message that returns the value of the requested key.
     * It is sent by one of the neighbors and received by the joining data node.
     */
    public static class SendItemData implements Serializable {
        public Integer key;
        public Data itemData;

        /**
         * @param key the requested key
         * @param itemData the requested data
         */
        public SendItemData(Integer key, Data itemData) {
            this.key = key;
            this.itemData = itemData;
        }
    }

    /**
     * AnnounceJoin
     * A message that informs the group that the node is entering the group.
     * It is sent by the joining data node and received by the data nodes in the group.
     */
    public static class AnnounceJoin implements Serializable {
        public Integer nodeKey;

        /**
         * @param nodeKey the node key of the joining node
         */
        public AnnounceJoin(Integer nodeKey) {
            this.nodeKey = nodeKey;
        }
    }

    /* ------- LEAVE ------- */

    /**
     * AskToLeave
     * A message that start the leave operation.
     * It is sent by the system and received by a data node, that will leave.
     */
    public static class AskToLeave implements Serializable {
        public AskToLeave() {
        }
    }

    /**
     * AnnounceLeave
     * A message that informs the group that the node is leaving the system.
     * It is sent by the leaving node and received by the group.
     */
    public static class AnnounceLeave implements Serializable {
        public AnnounceLeave() {
        }
    }

    /**
     * NewData
     * A message that returns the item of the leaving node to the nodes of the group.
     * It is sent by the leaving node and received by the group.
     */
    public static class NewData implements Serializable {
        public Integer key;
        public Data data;

        /**
         * @param key the key
         * @param data the pair value-version
         */
        public NewData(Integer key, Data data) {
            this.key = key;
            this.data = data;
        }
    }

    /* ------- CRASH ------- */

    /**
     * AskCrash
     * A message that requests the data node to crash.
     * It is sent by the system and received by a node.
     */
    public static class AskCrash implements Serializable {
        public AskCrash() {}
    }

    /**
     * AskRecover
     * A message that requests the data node to recover.
     * It is sent by the system and received by a crashed node.
     */
    public static class AskRecover implements Serializable {
        public ActorRef node;

        /**
         * @param node the boostrap node
         */
        public AskRecover(ActorRef node) {
            this.node = node;
        }
    }

    /**
     * AskGroupToRecover
     * A message that requests the group of nodes.
     * It is sent by the crashed node and received by the bootstrap node.
     */
    public static class AskGroupToRecover implements Serializable {
        public AskGroupToRecover() {
        }
    }

    /**
     * SendGroupToRecover
     * A message that returns the group of nodes.
     * It is sent by the boostrap node and received by the crashed node.
     */
    public static class SendGroupToRecover implements Serializable {
        public final List<DataNodeRef> group;

        /**
         * @param group the ActorRef list of the group
         */
        public SendGroupToRecover(List<DataNodeRef> group) {
            this.group = Collections.unmodifiableList(new ArrayList<>(group));
        }
    }

    /**
     * AskDataToRecover
     * A message that requests the data associated with the key to recover.
     * It is sent by the crashed node and received by its neighbors.
     */
    public static class AskDataToRecover implements Serializable {
        public Integer crashedNodeId;

        /**
         * @param nodeId the node id of the crashed node
         */
        public AskDataToRecover(Integer nodeId) {
            this.crashedNodeId = nodeId;
        }
    }

    /**
     * SendDataToRecover
     * A message that returns the data associated with the key to recover.
     * It is sent by one of the neighbors and received by the crashed node.
     */
    public static class SendDataToRecover implements Serializable {
        public Map<Integer, Data> data;

        /**
         * @param data the requested data
         */
        public SendDataToRecover(Map<Integer, Data> data) {
            this.data = Collections.unmodifiableMap(new HashMap<>(data));
        }
    }

    /**
     * TimeoutRecover
     * A message that returns a timeout during a recover operation.
     * It is sent by the recovering node to itself to start the timeout for the recovery action
     */
    public static class TimeoutRecover implements Serializable {
        public TimeoutRecover() {
        }
    }

    /**
     * TimeoutSendVersion
     * A message that returns a timeout while waiting for the version
     * It is sent by the node asking the version to itself to start the timeout for the action of asking the version during the update procedure
     */
    public static class TimeoutSendVersion implements Serializable {
        public Integer key;

        /**
         * @param key the requested key
         */
        public TimeoutSendVersion(Integer key) {
            this.key = key;
        }
    }

    /* ------- HANDLERS ------- */

    /**
     * InitializeDataGroup handler.
     * Initialize the group of nodes.
     * @param msg InitializeDataGroup msg
     * @see InitializeDataGroup
     */
    public void onInitializeDataGroup(InitializeDataGroup msg) {
        groupManager.addNode(msg.group);

        // logging
        Logs.init_group(Helper.getName(self()));
    }

    /* ------- WRITE ------- */

    /**
     * AskWriteData handler
     * Sends a WriteData message to the nodes that holds the key.
     * @param msg AskWriteData message
     * @see AskWriteData
     */
    public void onAskWriteData(AskWriteData msg) {
        for (ActorRef node : groupManager.findDataNodes(msg.key)) {
            WriteData data = new WriteData(msg.key, msg.value);
            node.tell(data, self());
        }

        // logging
        Logs.ask_write(msg.key, msg.value, Helper.getName(getSender()), Helper.getName(self()));
    }

    /**
     * WriteData handler.
     * Performs the write operation.
     * @param msg WriteData message
     * @see WriteData
     */
    public void onWriteData(WriteData msg) {
        if (!nodeData.isPresent(msg.key)) {
            nodeData.put(msg.key, msg.value);
            DataManager.Data elem = nodeData.getData(msg.key);

            // logging
            Logs.write(msg.key, elem.getValue(), Helper.getName(getSender()), Helper.getName(self()));
        } else {
            Logs.error(ErrorType.EXISTING_KEY, msg.key, Helper.getName(self()));
        }
    }

    /* ------- READ ------- */

    /**
     * AskReadData handler
     * Sends a read request to the node that holds the key.
     * Schedule the timeout message.
     * @param msg AskReadData message
     * @see AskReadData
     */
    public void onAskReadData(AskReadData msg) {
        requestManager.newReadReq(msg.requestId, getSender());
        for (ActorRef node : groupManager.findDataNodes(msg.key)) {
            ReadData request = new ReadData(msg.key, msg.requestId);
            node.tell(request, self());
        }

        // logging
        Logs.ask_read(msg.key, msg.requestId, Helper.getName(getSender()), Helper.getName(self()));

        getContext().system().scheduler().scheduleOnce(
                Duration.create(maxTimeout, TimeUnit.MILLISECONDS),
                getSelf(),
                new TimeoutOnRead(msg.requestId),
                getContext().system().dispatcher(), getSelf()
        );

    }

    /**
     * ReadData handler
     * Gets the value associated with the key.
     * @param msg ReadData message
     * @see ReadData
     */
    public void onReadData(ReadData msg) {
        if (nodeData.isPresent(msg.key)) {
            if (!nodeData.isBlocked(msg.key)) {
                Data readedData = nodeData.getData(msg.key);
                getSender().tell(new SendRead(readedData, msg.requestId), self());

                // logging
                Logs.read(msg.key, msg.requestId, Helper.getName(getSender()), Helper.getName(self()));
            } else {
                // data is locked
                Logs.error(ErrorType.LOCKED_KEY, msg.key, Helper.getName(sender()));
            }
        } else {
            // data is not present
            Logs.error(ErrorType.UNKNOWN_KEY, msg.key, Helper.getName(sender()));
        }
    }

    /**
     * SendRead handler.
     * Adds the data to the read quorum.
     * If the quorum is reached, the data is sent to the client.
     * If the quorum is not reached, nothing happens.
     * @param msg SendRead message
     * @see SendRead
     */
    public void onSendRead(SendRead msg) {
        switch (requestManager.addReadResp(msg.requestId, msg.data)) {
            case OK -> {
                // System.out.println("sending");
                ActorRef client = requestManager.getClientReadReq(msg.requestId);
                String requestedValue = requestManager.getReadValue(msg.requestId);
                requestManager.removeReadReq(msg.requestId);
                SendRead2Client resp = new SendRead2Client(requestedValue, msg.requestId);
                client.tell(resp, self());

                // logging
                Logs.read_reply(msg.data.getValue(), msg.data.getVersion(), msg.requestId, Helper.getName(self()), client.path().name());
            }
            default -> {}
        }
    }

    /**
     * TimeoutOnRead handler
     * Forward timeout message to the client.
     * @param msg TimeoutOnRead message
     * @see TimeoutOnRead
     */
    public void onTimeoutOnRead(TimeoutOnRead msg) {
        if (requestManager.isTimeoutOnRead(msg.requestId)) {
            ActorRef client = requestManager.getClientReadReq(msg.requestId);
            requestManager.removeReadReq(msg.requestId);
            client.tell(new ReturnTimeoutOnRead(msg.requestId), self());
        }
    }

    /* ------- UPDATE ------- */

    /**
     * AskUpdateData handler.
     * Send an update request to the node that holds the key.
     * @param msg AskUpdateData message
     * @see AskUpdateData
     */
    public void onAskUpdateData(AskUpdateData msg) {
        requestManager.newWriteReq(msg.requestId, getSender(), msg.key, msg.value);
        for (ActorRef node : groupManager.findDataNodes(msg.key)) {
            AskVersion request = new AskVersion(msg.key, msg.requestId);
            node.tell(request, self());
        }

        getContext().system().scheduler().scheduleOnce(
                Duration.create(maxTimeout, TimeUnit.MILLISECONDS),
                getSelf(),
                new TimeoutOnUpdate(msg.requestId),
                getContext().system().dispatcher(), getSelf()
        );

        // logging
        Logs.ask_update(msg.key, msg.value, msg.requestId, Helper.getName(getSender()), Helper.getName(self()));
    }

    /**
     * AskVersion handler.
     * Sends back the version of the requested item if it is not locked.
     * Schedule a timeout message
     * @param msg AskVersion message
     * @see AskVersion
     */
    public void onAskVersion(AskVersion msg) {
        if (nodeData.isPresent(msg.key)) {
            if (!nodeData.isBlocked(msg.key)) {
                Data readedData = nodeData.getDataAndBlock(msg.key);
                getSender().tell(new SendVersion(readedData.getVersion(), msg.requestId), self());

                getContext().system().scheduler().scheduleOnce(
                        Duration.create(maxTimeout, TimeUnit.MILLISECONDS),
                        getSelf(),
                        new TimeoutSendVersion(readedData.getVersion()),
                        getContext().system().dispatcher(), getSelf()
                );

                // logging
                Logs.ask_version(msg.key, msg.requestId, Helper.getName(getSender()), Helper.getName(self()));
            } else {
                // data is locked
                Logs.error(ErrorType.LOCKED_KEY, msg.key, Helper.getName(self()));
            }
        } else {
            // data is not present
            Logs.error(ErrorType.UNKNOWN_KEY, msg.key, Helper.getName(self()));
        }
    }

    /**
     * SendVersion handler.
     * If the quorum is reached, send the update message with the new value and version.
     * Return to the client the new version of the item.
     * @param msg SendVersion message
     * @see SendVersion
     */
    public void onSendVersion(SendVersion msg) {
        switch (requestManager.addWriteResp(msg.requestId, msg.version)) {
            case OK -> {
                ActorRef client = requestManager.getClientWriteReq(msg.requestId);

                Integer key = requestManager.getNewKeyOnWrite(msg.requestId);
                String value = requestManager.getNewValueOnWrite(msg.requestId);
                Integer version = requestManager.getVersionOnWrite(msg.requestId);
                requestManager.removeWriteReq(msg.requestId);

                // increase the version to 1 in respect to the quored one
                version += 1;
                // send the fetched version to the client
                ReturnUpdate resp = new ReturnUpdate(version, msg.requestId);
                client.tell(resp, self());

                // tell all data nodes to write the updated data
                for (ActorRef node : groupManager.findDataNodes(key)) {
                    UpdateData data = new UpdateData(key, value, version);
                    node.tell(data, self());
                }

                // logging
                Logs.version_reply(msg.version, msg.requestId, Helper.getName(getSender()), Helper.getName(self()));
            }

            default -> {
            }
        }
    }

    /**
     * TimeoutOnUpdate handler.
     * Forward timeout message to the client.
     * @param msg TimeoutOnUpdate message
     * @see TimeoutOnUpdate
     */
    public void onTimeoutOnUpdate(TimeoutOnUpdate msg) {
        if (requestManager.isTimeoutOnWrite(msg.requestId)) {
            ActorRef client = requestManager.getClientWriteReq(msg.requestId);
            requestManager.removeWriteReq(msg.requestId);
            client.tell(new ReturnTimeoutOnWrite(msg.requestId), self());
        }
    }

    /**
     * UpdateData handler.
     * Performs the update and removes the lock on the resource.
     * @param msg UpdateData message
     * @see UpdateData
     */
    public void onUpdateData(UpdateData msg) {
        nodeData.putUpdateAndRemoveBlock(msg.key, msg.value, msg.version);
        DataManager.Data elem = nodeData.getData(msg.key);

        // logging
        Logs.update(msg.key, elem.getValue(), Helper.getName(getSender()), Helper.getName(self()));
        // System.out.println("DataNode " + Helper.getName(self()) + ": update data {" + msg.key + ",(" + elem.getValue() + "," + elem.getVersion() + ")} saved");
    }

    /* ------- JOIN ------- */

    /**
     * AskToJoin handler.
     * Asks the boostrap node the actual group.
     * @param msg AskToJoin message
     * @see AskToJoin
     */
    public void onAskToJoin(AskToJoin msg) {
        msg.bootstrappingNode.tell(new AskNodeGroup(), self());
    }

    /**
     * AskNodeGroup handler
     * Sends back the group.
     * @param msg AskNodeGroup message
     * @see AskNodeGroup
     */
    public void onAskNodeGroup(AskNodeGroup msg) {
        List<DataNodeRef> group = groupManager.getGroup();
        getSender().tell(new SendNodeGroup(group), self());

        // logging
        Logs.ask_group(Helper.getName(getSender()), Helper.getName(self()));
    }

    /**
     * SendNodeGroup handler.
     * Find the neighbors and ask for the items.
     * @param msg SendNodeGroup message
     * @see SendNodeGroup
     */
    public void onSendNodeGroup(SendNodeGroup msg) {
        groupManager.addNode(msg.group);
        ActorRef neighbor = groupManager.getClockwiseNeighbor(nodeKey);
        neighbor.tell(new AskItems(), self());

        // logging
        Logs.group_reply(Helper.getName(getSender()), Helper.getName(self()));
    }

    /**
     * AskItems handler.
     * Send back the items.
     * @param msg AskItems message
     * @see AskItems
     */
    public void onAskItems(AskItems msg) {
        Set<Integer> items = nodeData.getKeys();
        getSender().tell(new SendItems(items), self());

        // logging
        Logs.ask_keys(Helper.getName(getSender()), Helper.getName(self()));
    }

    /**
     * SendItems handler.
     * Upon receiving the items, ask for the related data.
     * @param msg SendItems message
     */
    public void onSendItems(SendItems msg) {
        this.joinManager = new JoinManager(groupManager.replicasCount, msg.keys);
        for (Integer dataKey : msg.keys) {
            for (ActorRef node : groupManager.findDataNodes(dataKey)) {
                AskItemData request = new AskItemData(dataKey);
                node.tell(request, self());
            }
        }

        // logging
        Logs.items_reply(msg.keys.toString(), Helper.getName(getSender()), Helper.getName(self()));
    }

    /**
     * AskItemData handler.
     * Send back the data related to the item.
     * @param msg AskItemData message
     */
    public void onAskItemData(AskItemData msg) {
        Data itemData = nodeData.getData(msg.key);
        getSender().tell(new SendItemData(msg.key, itemData), self());

        // logging
        Logs.ask_data(msg.key, Helper.getName(getSender()), Helper.getName(self()));

    }

    /**
     * SendItemData handler.
     * Store the data in the node and announce the join to the group.
     * @param msg SendItemData message
     * @see SendItemData
     */
    public void onSendItemData(SendItemData msg) {
        if (joinManager.addData(msg.key, msg.itemData)) {
            HashMap<Integer, Data> items = joinManager.getData();
            joinManager = null;
            for (Map.Entry<Integer, Data> entry : items.entrySet()) {
                Integer key = entry.getKey();
                Data itemData = entry.getValue();
                nodeData.putData(key, itemData);
            }
            for (ActorRef dataNode : groupManager.getGroupActorRef()) {
                dataNode.tell(new AnnounceJoin(nodeKey), self());
            }
            groupManager.addNode(new DataNodeRef(nodeKey, self()));  // add itself to his group
        }

        // logging
        Logs.data_reply(msg.key, msg.itemData, Helper.getName(getSender()), Helper.getName(self()));
    }

    /**
     * AnnounceJoin handler.
     * Add the node to the group.
     * @param msg AnnounceJoin message
     * @see AnnounceJoin
     */
    public void onAnnounceJoin(AnnounceJoin msg) {
        groupManager.addNode(new DataNodeRef(msg.nodeKey, getSender()));

        // check if the node needs to drop items
        dropUselessItems();

        // logging
        Logs.join(msg.nodeKey, Helper.getName(getSender()), Helper.getName(self()));
    }

    /* ------- LEAVE ------- */

    /**
     * AskToLeave handler.
     * Informs the group that the node is leaving and sends its items to the neighbors.
     * @param msg AskToLeave message
     * @see AskToLeave
     */
    public void onAskToLeave(AskToLeave msg) {
        // announce to others
        for (ActorRef node : groupManager.getGroupActorRef()) {
            node.tell(new AnnounceLeave(), self());
        }

        // send data to the node that will become responsible for
        groupManager.removeNode(self());
        nodeData.getAllData().forEach((k, v) -> {
            for (ActorRef node : groupManager.findDataNodes(k)) {
                node.tell(new NewData(k, v), self());
            }
        });

        // logging
        Logs.ask_leave(Helper.getName(getSender()), Helper.getName(self()));
    }

    /**
     * AnnounceLeave handler.
     * Remove the node from the group.
     * @param msg AnnounceLeave message
     * @see AnnounceLeave
     */
    public void onAnnounceLeave(AnnounceLeave msg) {
        // remove the sender
        groupManager.removeNode(getSender());

        // logging
        Logs.leave(Helper.getName(getSender()), Helper.getName(self()));
    }

    /**
     * NewData handler
     * Store data and items of the leaving node.
     * @param msg NewData message
     * @see NewData
     */
    public void onNewData(NewData msg) {
        nodeData.putNewData(msg.key, msg.data);
    }

    /* ------- CRASH ------- */

    /**
     * AskCrash handler.
     * Make the node crash.
     * @param msg AskCrash message
     * @see AskCrash
     */
    public void onAskCrash(AskCrash msg) {
        crash();

        // logging
        Logs.crash(Helper.getName(getSender()), Helper.getName(self()));
    }

    /* ------- RECOVER ------- */

    /**
     * AskRecover handler.
     * Asks the bootstrap node the group.
     * @param msg AskRecover message
     */
    public void onAskRecover(AskRecover msg) {
        msg.node.tell(new AskGroupToRecover(), self());

        // logging
        Logs.ask_recover(msg.node.path().name(), Helper.getName(getSender()), Helper.getName(self()));
    }

    /**
     * AskGroupToRecover handler.
     * Sends back the actual group.
     * @param msg AskGroupToRecover message
     */
    public void onAskGroupToRecover(AskGroupToRecover msg) {
        List<DataNodeRef> group = groupManager.getGroup();
        getSender().tell(new SendGroupToRecover(group), self());

        // logging
        Logs.ask_group(Helper.getName(getSender()), Helper.getName(self()));
    }

    /**
     * SendGroupToRecover handler.
     * Select the item for which it is responsible and ask for the data.
     * Schedule a timeout message.
     * @param msg SendGroupToRecover message
     */
    public void onSendGroupToRecover(SendGroupToRecover msg) {
        groupManager.addNewGroup(msg.group);
        dropUselessItems();

//        System.out.print("Client " + Helper.getName(self()));
        for (ActorRef node : groupManager.findNeighbors(nodeKey)) {
//            System.out.print("Client " + node.path().name());
            node.tell(new AskDataToRecover(nodeKey), self());
        }
        getContext().system().scheduler().scheduleOnce(
                Duration.create(maxTimeout, TimeUnit.MILLISECONDS),
                getSelf(),
                new TimeoutRecover(),
                getContext().system().dispatcher(), getSelf()
        );

        // logging
        Logs.group_reply(Helper.getName(getSender()), Helper.getName(self()));

    }

    /**
     * AskData handler.
     * Sends back the data of the requested item.
     * @param msg AskDataToRecover message
     */
    public void onAskDataToRecover(AskDataToRecover msg) {
        ActorRef crashedNode = getSender();
        Map<Integer, Data> dataToSend = nodeData.getAllData().entrySet().stream()
                .filter(item -> groupManager.findDataNodes(item.getKey()).contains(crashedNode))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        crashedNode.tell(new SendDataToRecover(dataToSend), self());

        // logging
        Logs.ask_data(msg.crashedNodeId, Helper.getName(getSender()), Helper.getName(self()));
    }

    /**
     * TimeoutRecover handler.
     * Make the node recover.
     * @param msg TimeoutRecover message
     */
    public void onTimeoutRecover(TimeoutRecover msg) {
        recover();

        // logging
        Logs.timeout(TimeoutType.RECOVER, "", Helper.getName(getSender()), Helper.getName(self()));
    }

    /**
     * TimeoutSendVersion handler.
     * Remove the lock from the key.
     * @param msg TimeoutSendVersion message
     * @see TimeoutSendVersion
     */
    public void onTimeoutSendVersion(TimeoutSendVersion msg) {
        nodeData.removeBlock(msg.key);
    }

    /**
     * SendDataToRecover handler.
     * Store the data.
     * @param msg SendDataToRecover message
     */
    public void onSendDataToRecover(SendDataToRecover msg) {
        nodeData.add(msg.data);

        // logging
        Logs.data_recover(msg.data, Helper.getName(getSender()), Helper.getName(self()));
    }

    /* ------- DEBUG & TESTING ------- */

    /**
     * AskStatus
     * Message for requesting a status check of the system.
     * It follows the status request from a client node.
     */
    public static class AskStatus {
        AskStatus() {
        }
    }

    /**
     * PrintStatus
     * Message for printing the actual status.
     * It follows the ask status request.
     */
    public static class PrintStatus {
        PrintStatus() {
        }
    }

    /**
     * AskStatus handler.
     * Tells every node of the group to print its status.
     * @param msg AskStatus message
     * @see AskStatus
     */
    public void onAskStatus(AskStatus msg) {
        for (ActorRef node : groupManager.getGroupActorRef()) {
            node.tell(new PrintStatus(), getSelf());
        }
    }

    /**
     * PrintStatus handler.
     * Print the content.
     * @param msg PrintStatus message.
     * @see PrintStatus
     */
    public void onPrintStatus(PrintStatus msg) {
        for (int i : nodeData.getKeys()) {
            Data tmp = nodeData.getData(i);
            Logs.status(i, tmp.getValue(), tmp.getVersion(), Helper.getName(self()));
        }
    }


    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(InitializeDataGroup.class, this::onInitializeDataGroup)
                .match(AskWriteData.class, this::onAskWriteData)
                .match(WriteData.class, this::onWriteData)
                .match(AskReadData.class, this::onAskReadData)
                .match(ReadData.class, this::onReadData)
                .match(TimeoutOnRead.class, this::onTimeoutOnRead)
                .match(SendRead.class, this::onSendRead)
                .match(AskUpdateData.class, this::onAskUpdateData)
                .match(AskVersion.class, this::onAskVersion)
                .match(SendVersion.class, this::onSendVersion)
                .match(TimeoutOnUpdate.class, this::onTimeoutOnUpdate)
                .match(UpdateData.class, this::onUpdateData)
                .match(AskToJoin.class, this::onAskToJoin)
                .match(AskNodeGroup.class, this::onAskNodeGroup)
                .match(SendNodeGroup.class, this::onSendNodeGroup)
                .match(AskItems.class, this::onAskItems)
                .match(SendItems.class, this::onSendItems)
                .match(AskItemData.class, this::onAskItemData)
                .match(SendItemData.class, this::onSendItemData)
                .match(AnnounceJoin.class, this::onAnnounceJoin)
                .match(AskToLeave.class, this::onAskToLeave)
                .match(AnnounceLeave.class, this::onAnnounceLeave)
                .match(NewData.class, this::onNewData)
                .match(AskCrash.class, this::onAskCrash)
                .match(AskGroupToRecover.class, this::onAskGroupToRecover)
                .match(AskDataToRecover.class, this::onAskDataToRecover)
                .match(TimeoutSendVersion.class, this::onTimeoutSendVersion)
                .match(AskStatus.class, this::onAskStatus) // DEBUG
                .match(PrintStatus.class, this::onPrintStatus) // DEBUG
                .build();
    }

    final AbstractActor.Receive crashed() {
        return receiveBuilder()
                .match(AskRecover.class, this::onAskRecover)
                .match(SendGroupToRecover.class, this::onSendGroupToRecover)
                .match(TimeoutRecover.class, this::onTimeoutRecover)
                .match(SendDataToRecover.class, this::onSendDataToRecover)
                .matchAny(msg -> {})
                .build();
    }
}
