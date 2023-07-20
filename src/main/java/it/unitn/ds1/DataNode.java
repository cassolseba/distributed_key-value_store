package it.unitn.ds1;
import akka.actor.*;
import it.unitn.ds1.GroupManager.DataNodeRef;
import it.unitn.ds1.logger.Logs;
import scala.concurrent.duration.Duration;
import java.util.concurrent.TimeUnit;
import it.unitn.ds1.DataManager.Data;

import java.io.Serializable;
import java.util.*;

public class DataNode extends AbstractActor {
    private final int maxTimeout; // in ms
    public final Integer nodeKey; // Node key
    private final DataManager nodeData;
    private final GroupManager groupM;
    private final RequestManager rManager;
    private JoinManager jManager;

    public DataNode(int W_quorum, int R_quorum, int N_replica, int maxTimeout, int nodeKey) {
        this.maxTimeout = maxTimeout;
        this.nodeKey = nodeKey;
        this.rManager = new RequestManager(W_quorum, R_quorum);
        this.nodeData = new DataManager();
        this.groupM = new GroupManager(N_replica);

        System.out.println("INIT_NODE - Name: " + self().path().name() + ", key: " + nodeKey);
    }

    static public Props props(int W_quorum, int R_quorum, int N_replica, int maxTimeout, int nodeKey) {
        return Props.create(DataNode.class, () -> new DataNode(W_quorum, R_quorum, N_replica, maxTimeout, nodeKey));
    }


    ////////////
    // MESSAGES
    ///////////

    // used to initialize the datanode group
    public static class InitializeDataGroup implements Serializable {
        public final List<DataNodeRef> group;
        public InitializeDataGroup(List<DataNodeRef> group) {
            this.group = Collections.unmodifiableList(new ArrayList<>(group));
        }
    }

    // sent by the client and received by the coordinator datanode
    // used to start write data procedure in the data nodes
    public static class AskWriteData implements Serializable {
        public final Integer key;
        public final String value;
        public AskWriteData(Integer key, String value) {
            this.key = key; this.value = value;
        }
    }

    // sent by the coordinator to the proper datanode
    // used to tell at the datanode to write the data
    public static class WriteData implements Serializable {
        public final Integer key;
        public final String value;
        public WriteData(Integer key, String value) {
            this.key = key; this.value = value;
        }
    }

    // sent by the client and received by the coordinator datanode
    // used to start the read data procedure in the data nodes
    // requestId used to know who to answer the reading to
    public static class AskReadData implements Serializable {
        public final Integer key;
        public final String requestId;
        public AskReadData(Integer key, String requestId) {
            this.key = key; this.requestId = requestId;
        }
    }

    // sent by the coordinator and received by the proper data nodes
    // used to request to read the data to the proper data nodes
    public static class ReadData implements Serializable {
        public final Integer key;
        public final String requestId;
        public ReadData(Integer key, String requestId) {
            this.key = key; this.requestId = requestId;
        }
    }

    // sent by the coordinator and received by the coordinator
    // used to set a timout on a reading procedure
    public static class TimeoutR implements Serializable {
        public final String requestId;
        public TimeoutR(String requestId) {
            this.requestId = requestId;
        }
    }

    // sent by the coordinator and received by the client
    // used to tell the client that a timeout error occurred on the specified
    // reading request
    public static class SendTimeoutR2Client implements Serializable {
        public final String requestId;
        public SendTimeoutR2Client(String requestId) {
            this.requestId = requestId;
        }
    }

    // sent by the data nodes and received by the coordinator
    // used to send the data requested with the read to the coordinator (?)
    public static class SendRead implements Serializable {
        public final Data data;
        public final String requestId;
        public SendRead(Data data, String requestId) {
            this.data = data; this.requestId = requestId;
        }
    }

    // sent by the coordinator to the client
    // used to send the properly read data to the client that requested it
    public static class SendRead2Client implements Serializable {
        public final String value;
        public final String requestId;
        public SendRead2Client(String value, String requestId) {
            this.value = value; this.requestId = requestId;
        }
    }

    // sent by the client and received by the coordinator datanode
    // used to start the update data procedure in the data nodes
    // requestId used to know who to answer the to
    public static class AskUpdateData implements Serializable {
        public final Integer key;
        public final String value;
        public final String requestId;
        public AskUpdateData(Integer key, String value, String requestId) {
            this.key = key; this.value = value; this.requestId = requestId;
        }
    }

    // sent by the coordinator and received by the proper data nodes
    // used to request to read the version of the specified data to the proper data nodes
    public static class AskVersion implements Serializable {
        public final Integer key;
        public final String requestId;
        public AskVersion(Integer key, String requestId) {
            this.key = key; this.requestId = requestId;
        }
    }

    // sent by the data nodes and received by the coordinator
    // used to send the read version of the specified data to the coordinator
    public static class SendVersion implements Serializable {
        public final Integer version;
        public final String requestId;
        public SendVersion(Integer version, String requestId) {
            this.version = version; this.requestId = requestId;
        }
    }

    // sent by the coordinator and received by the coordinator
    // used to set a timout on a reading procedure
    public static class TimeoutW implements Serializable {
        public final String requestId;
        public TimeoutW(String requestId) {
            this.requestId = requestId;
        }
    }

    // sent by the coordinator and received by the client
    // used to tell the client that a timeout error occurred on the specified
    // reading request
    public static class SendTimeoutW2Client implements Serializable {
        public final String requestId;
        public SendTimeoutW2Client(String requestId) {
            this.requestId = requestId;
        }
    }

    // sent by the coordinator to the client
    // used to send the version of the updated data to the client that requested it
    public static class SendUpdate2Client implements Serializable {
        public final Integer version;
        public final String requestId;
        public SendUpdate2Client(Integer version, String requestId) {
            this.version = version; this.requestId = requestId;
        }
    }

    // sent by the coordinator to the proper data nodes
    // used to tell to the proper datanode to update the specified data
    public static class UpdateData implements Serializable {
        public final Integer key;
        public final String value;
        public final Integer version;
        public UpdateData(Integer key, String value, Integer version) {
            this.key = key; this.value = value; this.version = version;
        }
    }

    // sent by the joining datanode to the bootstrapping node
    // used by a joining datanode to request the datanode group
    public static class AskNodeGroup implements Serializable {
        public AskNodeGroup() {}
    }

    // sent by the bootstrapping node to the joining datanode
    // used to reply the ask for the group
    public static class SendNodeGroup implements Serializable {
        public final List<DataNodeRef> group;
        public SendNodeGroup(List<DataNodeRef> group) {
            this.group = Collections.unmodifiableList(new ArrayList<>(group));
        }
    }

    public static class AskDataToJoin implements Serializable {
        public AskDataToJoin() {}
    }

    public static class AskItems implements Serializable {
        public AskItems() {}
    }

    public static class SendItems implements Serializable {
        public final Set<Integer> keys;
        public SendItems(Set<Integer> keys) {
            this.keys = Collections.unmodifiableSet(new HashSet<>(keys));
        }
    }

    public static class AskItemData implements Serializable {
        public Integer key;
        public AskItemData(Integer key) {
            this.key = key;
        }
    }

    public static class SendItemData implements Serializable {
        public Integer key;
        public Data itemData;
        public SendItemData(Integer key, Data itemData) {
            this.key = key;
            this.itemData = itemData;
        }
    }

    public static class AnnounceJoin implements Serializable {
        Integer nodeKey;
        public AnnounceJoin(Integer nodeKey) {
            this.nodeKey = nodeKey;
        }
    }

    ////////////
    // HANDLERS
    ////////////

    public void onInitializeDataGroup(InitializeDataGroup msg) {
        groupM.add(msg.group);

        // logging
        Logs.init_group(self().path().name());
    }

    public void onAskWriteData(AskWriteData msg) {
        for (ActorRef node : groupM.findDataNodes(msg.key)) {
            WriteData data = new WriteData(msg.key, msg.value);
            node.tell(data, self());
        }

        // logging
        Logs.ask_write(msg.key, msg.value, getSender().path().name(), self().path().name());
    }

    public void onWriteData(WriteData msg) {
        nodeData.put(msg.key, msg.value);
        DataManager.Data elem = nodeData.get(msg.key);
        //System.out.println("DataNode " + self().path().name() + ": data {" + msg.key + ",(" + elem.getValue() + "," + elem.getVersion() + ")} saved");

        // logging
        Logs.write(msg.key, msg.value, getSender().path().name(), self().path().name());
    }

    public void onAskReadData(AskReadData msg) {
        rManager.createR(msg.requestId, getSender());
        for (ActorRef node : groupM.findDataNodes(msg.key)) {
            ReadData request = new ReadData(msg.key, msg.requestId);
            node.tell(request, self());
        }

        // logging
        Logs.ask_read(msg.key, msg.requestId, getSender().path().name(), self().path().name());

        getContext().system().scheduler().scheduleOnce(
            Duration.create(maxTimeout, TimeUnit.MILLISECONDS),
            getSelf(),
            new TimeoutR(msg.requestId),
            getContext().system().dispatcher(), getSelf()
        );

    }

    public void onReadData(ReadData msg) {
        Data readedData = nodeData.get(msg.key);
        getSender().tell(new SendRead(readedData, msg.requestId), self());

        // logging
        Logs.read(msg.key, msg.requestId, getSender().path().name(), self().path().name());
    }

    public void onSendRead(SendRead msg) {
        switch (rManager.addR(msg.requestId, msg.data)) {
            case OK -> {
                // System.out.println("sending");
                ActorRef client = rManager.getActorRefR(msg.requestId);
                String requestedValue = rManager.getValueR(msg.requestId);
                rManager.removeR(msg.requestId);
                SendRead2Client resp = new SendRead2Client(requestedValue, msg.requestId);
                client.tell(resp, self());

                // logging
                Logs.read_reply(msg.data.getValue(), msg.data.getVersion(), msg.requestId, self().path().name(), client.path().name());
            }
            default -> {}
        }
    }

    public void onTimeoutR(TimeoutR msg) {
        if(rManager.receiveTimeoutR(msg.requestId)) {
            ActorRef client = rManager.getActorRefR(msg.requestId);
            rManager.removeR(msg.requestId);
            client.tell(new SendTimeoutR2Client(msg.requestId), self());
        }
    }

    public void onAskUpdateData(AskUpdateData msg) {
        rManager.createW(msg.requestId, getSender(), msg.key, msg.value);
        for (ActorRef node : groupM.findDataNodes(msg.key)) {
            AskVersion request = new AskVersion(msg.key, msg.requestId);
            node.tell(request, self());
        }
    }

    public void onAskVersion(AskVersion msg) {
        Data readedData = nodeData.get(msg.key);
        getSender().tell(new SendVersion(readedData.getVersion(), msg.requestId), self());
    }

    public void onSendVersion(SendVersion msg) {
        switch (rManager.addW(msg.requestId, msg.version)) {
            case OK -> {
                ActorRef client = rManager.getActorRefW(msg.requestId);

                Integer key = rManager.getUpdateKeyW(msg.requestId);
                String value = rManager.getUpdateValueW(msg.requestId);
                Integer version = rManager.getVersionW(msg.requestId);
                rManager.removeW(msg.requestId);

                // increase the version to 1 in respect to the quored one
                version += 1;
                // send the fetched version to the client
                SendUpdate2Client resp = new SendUpdate2Client(version, msg.requestId);
                client.tell(resp, self());

                // tell all data nodes to write the updated data
                for (ActorRef node : groupM.findDataNodes(key)) {
                    UpdateData data = new UpdateData(key, value, version);
                    node.tell(data, self());
                }
            }

            default -> {}
        }
    }

    public void onTimeoutW(TimeoutW msg) {
        if(rManager.receiveTimeoutW(msg.requestId)) {
            ActorRef client = rManager.getActorRefW(msg.requestId);
            rManager.removeW(msg.requestId);
            client.tell(new SendTimeoutW2Client(msg.requestId), self());
        }
    }

    public void onUpdateData(UpdateData msg) {
        nodeData.putUpdate(msg.key, msg.value, msg.version);
        DataManager.Data elem = nodeData.get(msg.key);
        System.out.println("DataNode " + self().path().name() + ": update data {" + msg.key + ",(" + elem.getValue() + "," + elem.getVersion() + ")} saved");
    }

    public void onAskNodeGroup(AskNodeGroup msg) {
        List<DataNodeRef> group = groupM.getGroup();
        getSender().tell(new SendNodeGroup(group), self());
    }

    public void onSendNodeGroup(SendNodeGroup msg) {
        groupM.add(msg.group);
        ActorRef neighbor = groupM.getClockwiseNeighbor(nodeKey);
        neighbor.tell(new AskItems(), self());
    }

    public void onAskItems(AskItems msg) {
        Set<Integer> items = nodeData.getKeys();
        getSender().tell(new SendItems(items), self());
    }

    public void onSendItems(SendItems msg) {
        this.jManager = new JoinManager(groupM.N_replica, msg.keys);
        for (Integer dataKey : msg.keys) {
            for (ActorRef node : groupM.findDataNodes(dataKey)) {
                AskItemData request = new AskItemData(dataKey);
                node.tell(request, self());
            }
        }
    }

    public void onAskItemData(AskItemData msg) {
        Data itemData = nodeData.get(msg.key);
        getSender().tell(new SendItemData(msg.key, itemData), self());
    }

    public void onSendItemData(SendItemData msg) {
        if (jManager.add(msg.key, msg.itemData)) {
            HashMap<Integer, Data> items = jManager.getData();
            jManager = null;
            for (Map.Entry<Integer, Data> entry : items.entrySet()) {
                Integer key = entry.getKey();
                Data itemData = entry.getValue();
                nodeData.putData(key, itemData);
            }
            for (ActorRef dataNode : groupM.getGroupActorRef()) {
                dataNode.tell(new AnnounceJoin(nodeKey), self());
            }
            groupM.add(new DataNodeRef(nodeKey, self()));  // add itself to his group
        }
    }

    public void onAnnounceJoin(AnnounceJoin msg) {
        groupM.add(new DataNodeRef(msg.nodeKey, getSender()));

        // check if the node need to drop items
        // can be optimized
        nodeData.getKeys().removeIf(item -> !groupM.findDataNodes(item).contains(self()));
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
            .match(InitializeDataGroup.class, this::onInitializeDataGroup)
            .match(AskWriteData.class, this::onAskWriteData)
            .match(WriteData.class, this::onWriteData)
            .match(AskReadData.class, this::onAskReadData)
            .match(ReadData.class, this::onReadData)
            .match(TimeoutR.class, this::onTimeoutR)
            .match(SendRead.class, this::onSendRead)
            .match(AskUpdateData.class, this::onAskUpdateData)
            .match(AskVersion.class, this::onAskVersion)
            .match(SendVersion.class, this::onSendVersion)
            .match(TimeoutW.class, this::onTimeoutW)
            .match(UpdateData.class, this::onUpdateData)
            .match(AskNodeGroup.class, this::onAskNodeGroup)
            .match(SendNodeGroup.class, this::onSendNodeGroup)
            .match(AskItems.class, this::onAskItems)
            .match(SendItems.class, this::onSendItems)
            .match(AskItemData.class, this::onAskItemData)
            .match(SendItemData.class, this::onSendItemData)
            .match(AnnounceJoin.class, this::onAnnounceJoin)
            .build();
    }
}
