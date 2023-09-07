package it.unitn.ds1.actors;

import akka.actor.*;
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
 * Represent a data node in the system.
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
     * Make the data node crash.
     */
    private void crash() {
        getContext().become(crashed());
    }

    /**
     * Make the data node recover.
     */
    private void recover() {
        getContext().become(createReceive());
    }

    /**
     * Drop the items that are not in the group anymore.
     */
    private void dropUselessItems() {
        nodeData.getKeys().removeIf(item -> !groupManager.findDataNodes(item).contains(self()));
    }

    /* ------- MESSAGES ------- */

    /**
     * A message that initializes the group of nodes in this node.
     */
    public static class InitializeDataGroup implements Serializable {
        public final List<DataNodeRef> group;

        public InitializeDataGroup(List<DataNodeRef> group) {
            this.group = Collections.unmodifiableList(new ArrayList<>(group));
        }
    }

    /**
     * A message that starts the write operation in the data nodes.
     * It is sent by the client and received by the coordinator data node.
     */
    public static class AskWriteData implements Serializable {
        public final Integer key;
        public final String value;

        public AskWriteData(Integer key, String value) {
            this.key = key;
            this.value = value;
        }
    }

    /**
     * A message that tells the datanode to write the data.
     * It is sent by the coordinator and received by the proper datanode.
     */
    public static class WriteData implements Serializable {
        public final Integer key;
        public final String value;

        public WriteData(Integer key, String value) {
            this.key = key;
            this.value = value;
        }
    }

    /**
     * A message that starts the read operation in the data nodes.
     * It is sent by the client and received by the coordinator data node.
     * The requestId is used to know who to answer the read operation.
     */
    public static class AskReadData implements Serializable {
        public final Integer key;
        public final String requestId;

        public AskReadData(Integer key, String requestId) {
            this.key = key;
            this.requestId = requestId;
        }
    }

    /**
     * A message that tells the datanode to read the data.
     * It is sent by the coordinator and received by the proper datanode.
     */
    public static class ReadData implements Serializable {
        public final Integer key;
        public final String requestId;

        public ReadData(Integer key, String requestId) {
            this.key = key;
            this.requestId = requestId;
        }
    }

    /**
     * A message that set a timeout for the datanode during a read operation.
     * It is sent by the coordinator and received by the coordinator.
     */
    public static class TimeoutOnRead implements Serializable {
        public final String requestId;

        public TimeoutOnRead(String requestId) {
            this.requestId = requestId;
        }
    }

    /**
     * A message that tells the client that a timeout occurred during a certain read operation.
     * It is sent by the coordinator and received by the client.
     */
    public static class ReturnTimeoutOnRead implements Serializable {
        public final String requestId;

        public ReturnTimeoutOnRead(String requestId) {
            this.requestId = requestId;
        }
    }

    /**
     * A message that tells the result of a certain read operation to the coordinator.
     * It is sent by the data nodes and received by the coordinator.
     */
    public static class SendRead implements Serializable {
        public final Data data;
        public final String requestId;

        public SendRead(Data data, String requestId) {
            this.data = data;
            this.requestId = requestId;
        }
    }

    /**
     * A message that tells the result of a certain read operation to the client.
     * It is sent by the coordinator and received by the client.
     */
    public static class SendRead2Client implements Serializable {
        public final String value;
        public final String requestId;

        public SendRead2Client(String value, String requestId) {
            this.value = value;
            this.requestId = requestId;
        }
    }

    /**
     * A message that starts the update data procedure in the data nodes.
     * It is sent by the client and received by the coordinator datanode.
     * The requestId is used to know who to answer the update operation.
     */
    public static class AskUpdateData implements Serializable {
        public final Integer key;
        public final String value;
        public final String requestId;

        public AskUpdateData(Integer key, String value, String requestId) {
            this.key = key;
            this.value = value;
            this.requestId = requestId;
        }
    }

    /**
     * A message that ask the datanode the version of the specified data.
     * It is sent by the coordinator and received by the proper datanode.
     */
    public static class AskVersion implements Serializable {
        public final Integer key;
        public final String requestId;

        public AskVersion(Integer key, String requestId) {
            this.key = key;
            this.requestId = requestId;
        }
    }

    /**
     * A message that tells the version of the specified data to the coordinator.
     * It is sent by the data nodes and received by the coordinator.
     */
    public static class SendVersion implements Serializable {
        public final Integer version;
        public final String requestId;

        public SendVersion(Integer version, String requestId) {
            this.version = version;
            this.requestId = requestId;
        }
    }

    /**
     * A message that set a timeout for the datanode during a write operation.
     * It is sent by the coordinator and received by the coordinator.
     */
    public static class TimeoutOnWrite implements Serializable {
        public final String requestId;

        public TimeoutOnWrite(String requestId) {
            this.requestId = requestId;
        }
    }

    /**
     * A message that tells the client that a timeout occurred during a certain write operation.
     * It is sent by the coordinator and received by the client.
     */
    public static class ReturnTimeoutOnWrite implements Serializable {
        public final String requestId;

        public ReturnTimeoutOnWrite(String requestId) {
            this.requestId = requestId;
        }
    }

    /**
     * A message that tells the result of a certain write operation to the coordinator.
     * It is sent by the coordinator and received by the client.
     */
    public static class ReturnUpdate implements Serializable {
        public final Integer version;
        public final String requestId;

        public ReturnUpdate(Integer version, String requestId) {
            this.version = version;
            this.requestId = requestId;
        }
    }

    /**
     * A message that tells the data node to update the specified data.
     * It is sent by the coordinator and received by the proper data node.
     */
    public static class UpdateData implements Serializable {
        public final Integer key;
        public final String value;
        public final Integer version;

        public UpdateData(Integer key, String value, Integer version) {
            this.key = key;
            this.value = value;
            this.version = version;
        }
    }

    /**
     * A message that requests the group of nodes to the bootstrapping node.
     * It is sent by the joining data node and received by the bootstrapping node.
     */
    public static class AskNodeGroup implements Serializable {
        public AskNodeGroup() {
        }
    }

    /**
     * A message that starts the join operation.
     * It is sent by the joining data node and received by the bootstrapping node.
     */
    public static class AskToJoin implements Serializable {
        public ActorRef bootstrappingNode;

        public AskToJoin(ActorRef node) {
            this.bootstrappingNode = node;
        }
    }

    /**
     * A message that returns the group of nodes to the joining data node.
     * It is sent by the bootstrapping node and received by the joining data node.
     */
    public static class SendNodeGroup implements Serializable {
        public final List<DataNodeRef> group;

        public SendNodeGroup(List<DataNodeRef> group) {
            this.group = Collections.unmodifiableList(new ArrayList<>(group));
        }
    }

    /**
     * TODO ??
     */
    public static class AskDataToJoin implements Serializable {
        public AskDataToJoin() {
        }
    }

    /**
     * A message that requests the set of keys.
     */
    public static class AskItems implements Serializable {
        public AskItems() {
        }
    }

    /**
     * A message that returns the set of keys.
     */
    public static class SendItems implements Serializable {
        public final Set<Integer> keys;

        public SendItems(Set<Integer> keys) {
            this.keys = Collections.unmodifiableSet(new HashSet<>(keys));
        }
    }

    /**
     * A message that requests the data associated with the specified key.
     * It is sent by the joining data node and received by the proper data node.
     */
    public static class AskItemData implements Serializable {
        public Integer key;

        public AskItemData(Integer key) {
            this.key = key;
        }
    }

    /**
     * A message that returns the data associated with the specified key.
     * It is sent by the proper data node and received by the joining data node.
     */
    public static class SendItemData implements Serializable {
        public Integer key;
        public Data itemData;

        public SendItemData(Integer key, Data itemData) {
            this.key = key;
            this.itemData = itemData;
        }
    }

    /**
     * A message that announces the joining of a new data node.
     * It is sent by the joining data node and received by the data nodes in the group.
     */
    public static class AnnounceJoin implements Serializable {
        public Integer nodeKey;

        public AnnounceJoin(Integer nodeKey) {
            this.nodeKey = nodeKey;
        }
    }

    /**
     * A message that requests the leave operation.
     */
    public static class AskToLeave implements Serializable {
        public AskToLeave() {
        }
    }

    /**
     * A message that announces the leaving of a data node.
     */
    public static class AnnounceLeave implements Serializable {
        public AnnounceLeave() {
        }
    }

    /**
     * TODO ??
     */
    public static class NewData implements Serializable {
        public Integer key;
        public Data data;

        public NewData(Integer key, Data data) {
            this.key = key;
            this.data = data;
        }
    }

    /**
     * A message that requests the data node to crash.
     */
    public static class AskCrash implements Serializable {
        public AskCrash() {
        }
    }

    /**
     * A message that requests the data node to recover.
     */
    public static class AskRecover implements Serializable {
        public ActorRef node;

        public AskRecover(ActorRef node) {
            this.node = node;
        }
    }

    /**
     * A message that requests the group of nodes to recover.
     */
    public static class AskGroupToRecover implements Serializable {
        public AskGroupToRecover() {
        }
    }

    /**
     * A message that returns the group of nodes to recover.
     */
    public static class SendGroupToRecover implements Serializable {
        public final List<DataNodeRef> group;

        public SendGroupToRecover(List<DataNodeRef> group) {
            this.group = Collections.unmodifiableList(new ArrayList<>(group));
        }
    }

    /**
     * A message that requests the data associated with the specified key to recover.
     */
    public static class AskDataToRecover implements Serializable {
        public Integer crashedNodeId;

        public AskDataToRecover(Integer nodeId) {
            this.crashedNodeId = nodeId;
        }
    }

    /**
     * A message that returns the data associated with the specified key to recover.
     */
    public static class SendDataToRecover implements Serializable {
        public Map<Integer, Data> data;

        public SendDataToRecover(Map<Integer, Data> data) {
            this.data = Collections.unmodifiableMap(new HashMap<>(data));
        }
    }

    /**
     * TODO ??
     */
    public static class TimeoutRecover implements Serializable {
        public TimeoutRecover() {
        }
    }

    /* ------- HANDLERS ------- */

    /**
     * Handler that set the actual group of nodes in this node.
     *
     * @param msg InitializeDataGroup msg
     * @see InitializeDataGroup
     */
    public void onInitializeDataGroup(InitializeDataGroup msg) {
        groupManager.addNode(msg.group);

        // logging
        Logs.init_group(Helper.getName(self()));
    }

    /**
     * Handler that, on receiving an AskWriteData message,
     * sends a WriteData message to the nodes that have the specified key.
     *
     * @param msg AskWriteData message
     * @see AskWriteData
     * @see WriteData
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
     * Handler that, on receiving a WriteData message,
     * writes the pair {key, value} in this nodeData
     *
     * @param msg WriteData message
     * @see WriteData
     * @see Data
     */
    public void onWriteData(WriteData msg) {
        nodeData.put(msg.key, msg.value);
        DataManager.Data elem = nodeData.getData(msg.key);
        //System.out.println("DataNode " + Helper.getName(self()) + ": data {" + msg.key + ",(" + elem.getValue() + "," + elem.getVersion() + ")} saved");

        // logging
        Logs.write(msg.key, elem.getValue(), Helper.getName(getSender()), Helper.getName(self()));
    }

    /**
     * Handler that, on receiving an AskReadData message,
     * sends a ReadData message to the nodes that have the specified key.
     *
     * @param msg AskReadData message
     * @see AskReadData
     * @see ReadData
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
     * Handler that, on receiving a ReadData message,
     * gets the value from this nodeData associated with the provided key.
     *
     * @param msg WriteData message
     * @see ReadData
     * @see Data
     */
    public void onReadData(ReadData msg) {
        Data readedData = nodeData.getData(msg.key);
        getSender().tell(new SendRead(readedData, msg.requestId), self());

        // logging
        Logs.read(msg.key, msg.requestId, Helper.getName(getSender()), Helper.getName(self()));
    }

    /**
     * Handler that, on receiving a SendRead message,
     * adds the data to the read quorum.
     * If the quorum is reached, the data is sent to the client.
     * If the quorum is not reached, nothing happens.
     * @param msg SendRead message
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
     * Handler that, on receiving a TimeoutOnRead message,
     * checks if the timeout is still valid.
     * If it is, the read operation is aborted.
     * @param msg TimeoutOnRead message
     */
    public void onTimeoutOnRead(TimeoutOnRead msg) {
        if (requestManager.isTimeoutOnRead(msg.requestId)) {
            ActorRef client = requestManager.getClientReadReq(msg.requestId);
            requestManager.removeReadReq(msg.requestId);
            client.tell(new ReturnTimeoutOnRead(msg.requestId), self());
        }
    }

    /**
     * Handler that, on receiving a onAskUpdateData message,
     * sends a AskVersion message to the nodes that have the specified key.
     * @param msg AskUpdateData message
     */
    public void onAskUpdateData(AskUpdateData msg) {
        requestManager.newWriteReq(msg.requestId, getSender(), msg.key, msg.value);
        for (ActorRef node : groupManager.findDataNodes(msg.key)) {
            AskVersion request = new AskVersion(msg.key, msg.requestId);
            node.tell(request, self());
        }

        // logging
        Logs.ask_update(msg.key, msg.value, msg.requestId, Helper.getName(getSender()), Helper.getName(self()));
    }

    /**
     * Handler that, on receiving an AskVersion message,
     * gets the version from this nodeData associated with the provided key
     * and send back.
     * @param msg AskVersion message
     */
    public void onAskVersion(AskVersion msg) {
        Data readedData = nodeData.getData(msg.key);
        getSender().tell(new SendVersion(readedData.getVersion(), msg.requestId), self());

        // logging
        Logs.ask_version(msg.key, msg.requestId, Helper.getName(getSender()), Helper.getName(self()));
    }

    /**
     * Handler that, on receiving a SendVersion message,
     * adds the version to the write quorum.
     * If the quorum is reached, the data is sent to the client.
     * @param msg SendVersion message
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
     * Handler that, on receiving a TimeoutOnWrite message,
     * checks if the timeout is still valid.
     * If it is, the write operation is aborted.
     * @param msg TimeoutOnWrite message
     */
    public void onTimeoutOnWrite(TimeoutOnWrite msg) {
        if (requestManager.isTimeoutOnWrite(msg.requestId)) {
            ActorRef client = requestManager.getClientWriteReq(msg.requestId);
            requestManager.removeWriteReq(msg.requestId);
            client.tell(new ReturnTimeoutOnWrite(msg.requestId), self());
        }
    }

    /**
     * Handler that, on receiving an UpdateData message,
     * updates the data associated with the provided key.
     * @param msg UpdateData message
     */
    public void onUpdateData(UpdateData msg) {
        nodeData.putUpdate(msg.key, msg.value, msg.version);
        DataManager.Data elem = nodeData.getData(msg.key);

        // logging
        Logs.update(msg.key, elem.getValue(), Helper.getName(getSender()), Helper.getName(self()));
        // System.out.println("DataNode " + Helper.getName(self()) + ": update data {" + msg.key + ",(" + elem.getValue() + "," + elem.getVersion() + ")} saved");
    }

    /**
     * Handler that, on receiving an onAskToJoin message,
     * sends a AskNodeGroup message to the bootstrapping node.
     * @param msg AskToJoin message
     */
    public void onAskToJoin(AskToJoin msg) {
        msg.bootstrappingNode.tell(new AskNodeGroup(), self());
    }

    /**
     * Handler that, on receiving an AskNodeGroup message,
     * sends a SendNodeGroup message to the joining data node.
     * @param msg AskNodeGroup message
     */
    public void onAskNodeGroup(AskNodeGroup msg) {
        List<DataNodeRef> group = groupManager.getGroup();
        getSender().tell(new SendNodeGroup(group), self());

        // logging
        Logs.ask_group(Helper.getName(getSender()), Helper.getName(self()));
    }

    /**
     * Handler that, on receiving a SendNodeGroup message,
     * adds the group of nodes to this node
     * and sends an AskItems message to the clockwise neighbor.
     * @param msg SendNodeGroup message
     */
    public void onSendNodeGroup(SendNodeGroup msg) {
        groupManager.addNode(msg.group);
        ActorRef neighbor = groupManager.getClockwiseNeighbor(nodeKey);
        neighbor.tell(new AskItems(), self());

        // logging
        Logs.group_reply(Helper.getName(getSender()), Helper.getName(self()));
    }

    /**
     * Handler that, on receiving an AskItems message,
     * sends a SendItems message to the joining data node.
     * @param msg AskItems message
     */
    public void onAskItems(AskItems msg) {
        Set<Integer> items = nodeData.getKeys();
        getSender().tell(new SendItems(items), self());

        // logging
        Logs.ask_keys(Helper.getName(getSender()), Helper.getName(self()));
    }

    /**
     * Handler that, on receiving a SendItems message,
     * adds the items to the join manager
     * and sends an AskItemData message to the data nodes that have the specified key.
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
     * Handler that, on receiving an AskItemData message,
     * sends a SendItemData message to the joining data node.
     * @param msg AskItemData message
     */
    public void onAskItemData(AskItemData msg) {
        Data itemData = nodeData.getData(msg.key);
        getSender().tell(new SendItemData(msg.key, itemData), self());

        // logging
        Logs.ask_data(msg.key, Helper.getName(getSender()), Helper.getName(self()));

    }

    /**
     * Handler that, on receiving a SendItemData message,
     * adds the data to the join manager.
     * If the join manager has all the data, it sends an AnnounceJoin message to the data nodes in the group.
     * @param msg SendItemData message
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
     * Handler that, on receiving an AnnounceJoin message,
     * adds the node to the group.
     * @param msg AnnounceJoin message
     */
    public void onAnnounceJoin(AnnounceJoin msg) {
        groupManager.addNode(new DataNodeRef(msg.nodeKey, getSender()));

        // check if the node needs to drop items
        dropUselessItems();

        // logging
        Logs.join(msg.nodeKey, Helper.getName(getSender()), Helper.getName(self()));
    }

    /**
     * Handler that, on receiving an AskToLeave message,
     * sends an AnnounceLeave message to the data nodes in the group.
     * @param msg AskToLeave message
     */
    public void onAskToLeave(AskToLeave msg) {
        System.out.println("DataNode " + Helper.getName(self()) + " leaved");
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
     * Handler that, on receiving an AnnounceLeave message,
     * removes the node from the group.
     * @param msg AnnounceLeave message
     */
    public void onAnnounceLeave(AnnounceLeave msg) {
        // remove the sender
        groupManager.removeNode(getSender());

        // logging
        Logs.leave(Helper.getName(getSender()), Helper.getName(self()));
    }

    /**
     * Handler that, on receiving a NewData message,
     * adds the data to the nodeData.
     * @param msg NewData message
     */
    public void onNewData(NewData msg) {
        nodeData.putNewData(msg.key, msg.data);

        // logging
        // TODO
    }

    /**
     * Handler that, on receiving an AskCrash message,
     * crashes the node.
     * @param msg AskCrash message
     */
    public void onAskCrash(AskCrash msg) {
        crash();

        // logging
        Logs.crash(Helper.getName(getSender()), Helper.getName(self()));
    }

    /**
     * Handler that, on receiving an AskRecover message,
     * sends an AskGroupToRecover message to the bootstrapping node.
     * @param msg AskRecover message
     */
    public void onAskRecover(AskRecover msg) {
        msg.node.tell(new AskGroupToRecover(), self());

        // logging
        Logs.ask_recover(msg.node.path().name(), Helper.getName(getSender()), Helper.getName(self()));
    }

    /**
     * Handler that, on receiving an AskGroupToRecover message,
     * sends a SendGroupToRecover message to the crashed node.
     * @param msg AskGroupToRecover message
     */
    public void onAskGroupToRecover(AskGroupToRecover msg) {
        List<DataNodeRef> group = groupManager.getGroup();
        getSender().tell(new SendGroupToRecover(group), self());

        // logging
        Logs.ask_group(Helper.getName(getSender()), Helper.getName(self()));
    }

    /**
     * Handler that, on receiving a SendGroupToRecover message,
     * adds the group of nodes to this node
     * and sends an AskDataToRecover message to the data nodes that have the specified key.
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
     * Handler that, on receiving an AskDataToRecover message,
     * sends a SendDataToRecover message to the crashed node.
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
     * Handler that, on receiving a TimeoutRecover message,
     * recovers the node.
     * @param msg TimeoutRecover message
     */
    public void onTimeoutRecover(TimeoutRecover msg) {
        recover();

        // logging
        Logs.timeout(TimeoutType.RECOVER, "", Helper.getName(getSender()), Helper.getName(self()));
    }

    /**
     * Handler that, on receiving a SendDataToRecover message,
     * adds the data to the nodeData.
     * @param msg SendDataToRecover message
     */
    public void onSendDataToRecover(SendDataToRecover msg) {
        nodeData.add(msg.data);

        // logging
        Logs.data_recover(msg.data, Helper.getName(getSender()), Helper.getName(self()));
    }


    // DEBUG CLASSES AND FUNCTIONS
    // __________________________________________________________________________

    /**
     * Message for requesting a status check of the system. It follows the status request from a client node.
     */
    public static class AskStatus {
        AskStatus() {
        }
    }

    /**
     * Message for printing the actual status. It follows the ask status request.
     */
    public static class PrintStatus {
        PrintStatus() {
        }
    }

    /**
     * Ask status handler.
     *
     * @param msg is an AskStatus message.
     */
    public void onAskStatus(AskStatus msg) {
        System.out.println("\n---STATUS CHECK STARTING---\n");
        for (ActorRef node : groupManager.getGroupActorRef()) {
            node.tell(new PrintStatus(), getSelf());
        }
    }

    /**
     * Print status handler: iterate on the data stored in the node and print the content.
     *
     * @param msg is a PrintStatus message.
     */
    public void onPrintStatus(PrintStatus msg) {
        for (int i : nodeData.getKeys()) {
            Data tmp = nodeData.getData(i);
            Logs.status(i, tmp.getValue(), tmp.getVersion(), Helper.getName(self()));
        }
    }

    // __________________________________________________________________________
    // END DEBUG CLASSES AND FUNCTIONS

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
                .match(TimeoutOnWrite.class, this::onTimeoutOnWrite)
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
