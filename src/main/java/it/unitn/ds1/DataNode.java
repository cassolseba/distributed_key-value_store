package it.unitn.ds1;
import akka.actor.*;
import it.unitn.ds1.GroupManager.DataNodeRef;
import it.unitn.ds1.DataManager.Data;
import it.unitn.ds1.RequestManager.MRequestType;

import java.io.Serializable;
import java.util.*;

public class DataNode extends AbstractActor {
    private final int maxTimeout; // in ms
    public final Integer nodeKey; // Node key
    private final DataManager nodeData;
    private final GroupManager groupM;
    private final RequestManager rManager;

    public DataNode(int W_quorum, int R_quorum, int N_replica, int maxTimeout, int nodeKey) {
        this.maxTimeout = maxTimeout;
        this.nodeKey = nodeKey;
        this.rManager = new RequestManager(W_quorum, R_quorum);
        this.nodeData = new DataManager();
        this.groupM = new GroupManager(N_replica);

        System.out.println("DataNode " + self().path().name() + " created, nodeKey=" + nodeKey);
    }

    static public Props props(int W_quorum, int R_quorum, int N_replica, int maxTimeout, int nodeKey) {
        return Props.create(DataNode.class, () -> new DataNode(W_quorum, R_quorum, N_replica, maxTimeout, nodeKey));
    }


    ////////////
    // MESSAGES
    ///////////

    // Start message to initialize the datanode groups
    public static class InitializeDataGroup implements Serializable {
        public final List<DataNodeRef> group;
        public InitializeDataGroup(List<DataNodeRef> group) {
            this.group = Collections.unmodifiableList(new ArrayList<>(group));
        }
    }

    public static class AskWriteData implements Serializable {
        public final Integer key;
        public final String value;
        public AskWriteData(Integer key, String value) {
            this.key = key; this.value = value;
        }
    }

    public static class WriteData implements Serializable {
        public final Integer key;
        public final String value;
        public WriteData(Integer key, String value) {
            this.key = key; this.value = value;
        }
    }

    public static class AskReadData implements Serializable {
        public final Integer key;
        public final String requestId;
        public AskReadData(Integer key, String requestId) {
            this.key = key; this.requestId = requestId;
        }
    }

    public static class ReadData implements Serializable {
        public final Integer key;
        public final String requestId;
        public ReadData(Integer key, String requestId) {
            this.key = key; this.requestId = requestId;
        }
    }

    public static class SendRead implements Serializable {
        public final Data data;
        public final String requestId;
        public SendRead(Data data, String requestId) {
            this.data = data; this.requestId = requestId;
        }
    }

    public static class SendRead2Client implements Serializable {
        public final String value;
        public final String requestId;
        public SendRead2Client(String value, String requestId) {
            this.value = value; this.requestId = requestId;
        }
    }


    ////////////
    // HANDLERS
    ////////////

    public void onInitializeDataGroup(InitializeDataGroup msg) {
        groupM.add(msg.group);
    }

    public void onAskWriteData(AskWriteData msg) {
        for (ActorRef node : groupM.findDataNodes(msg.key)) {
            WriteData data = new WriteData(msg.key, msg.value);
            node.tell(data, self());
        }
    }

    public void onWriteData(WriteData msg) {
        nodeData.put(msg.key, msg.value);
        DataManager.Data elem = nodeData.get(msg.key);
        System.out.println("DataNode " + self().path().name() + ": data {" + msg.key + ",(" + elem.getValue() + "," + elem.getVersion() + ")} saved");
    }

    public void onAskReadData(AskReadData msg) {
        rManager.create(msg.requestId, getSender(), MRequestType.READER);
        for (ActorRef node : groupM.findDataNodes(msg.key)) {
            ReadData request = new ReadData(msg.key, msg.requestId);
            node.tell(request, self());
        }
    }

    public void onReadData(ReadData msg) {
        Data readedData = nodeData.get(msg.key);
        getSender().tell(new SendRead(readedData, msg.requestId), self());
    }

    public void onSendRead(SendRead msg) {
        switch (rManager.add(msg.requestId, msg.data)) {
            case OK -> {
                // System.out.println("sending");
                ActorRef client = rManager.getActorRef(msg.requestId);
                String requestedValue = rManager.getValueAndRemove(msg.requestId);
                SendRead2Client resp = new SendRead2Client(requestedValue, msg.requestId);
                client.tell(resp, self());
            }
            default -> {}
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
            .match(SendRead.class, this::onSendRead)
            .build();
    }
}
