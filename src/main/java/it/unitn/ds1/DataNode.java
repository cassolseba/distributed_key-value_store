package it.unitn.ds1;
import akka.actor.*;
import it.unitn.ds1.GroupManager.DataNodeRef;
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

    // used to initialize the datanode group
    public static class InitializeDataGroup implements Serializable {
        public final List<DataNodeRef> group;
        public InitializeDataGroup(List<DataNodeRef> group) {
            this.group = Collections.unmodifiableList(new ArrayList<>(group));
        }
    }

    // sended by the client and received by the coordinator datanode
    // used to start the write data procedure in the datanodes
    public static class AskWriteData implements Serializable {
        public final Integer key;
        public final String value;
        public AskWriteData(Integer key, String value) {
            this.key = key; this.value = value;
        }
    }

    // sended by the coordinator to the proper datanode
    // used to tell at the datanode to write the data
    public static class WriteData implements Serializable {
        public final Integer key;
        public final String value;
        public WriteData(Integer key, String value) {
            this.key = key; this.value = value;
        }
    }

    // sended by the client and received by the coordinator datanode
    // used to start the read data procedure in the datanodes
    // requestId used to know who to answer the reading to
    public static class AskReadData implements Serializable {
        public final Integer key;
        public final String requestId;
        public AskReadData(Integer key, String requestId) {
            this.key = key; this.requestId = requestId;
        }
    }

    // sended by the coordinator and received by the proper datanodes
    // used to request to read the data to the proper datanodes
    public static class ReadData implements Serializable {
        public final Integer key;
        public final String requestId;
        public ReadData(Integer key, String requestId) {
            this.key = key; this.requestId = requestId;
        }
    }

    // sended by the coordinator and received by the coordinator
    // used to set a timout on a reading procedure
    public static class TimeoutR implements Serializable {
        public final String requestId;
        public TimeoutR(String requestId) {
            this.requestId = requestId;
        }
    }

    // sended by the coordinator and received by the client
    // used to tell the client that a timeout error occurred on the specified
    // reading request
    public static class SendTimeoutR2Client implements Serializable {
        public final String requestId;
        public SendTimeoutR2Client(String requestId) {
            this.requestId = requestId;
        }
    }

    // sended by the datanodes and received by the coordinator
    // used to send the readed requested data to the coordinator
    public static class SendRead implements Serializable {
        public final Data data;
        public final String requestId;
        public SendRead(Data data, String requestId) {
            this.data = data; this.requestId = requestId;
        }
    }

    // sended by the coordinator to the client
    // used to send the properly readed data to the client that requested it
    public static class SendRead2Client implements Serializable {
        public final String value;
        public final String requestId;
        public SendRead2Client(String value, String requestId) {
            this.value = value; this.requestId = requestId;
        }
    }

    // sended by the client and received by the coordinator datanode
    // used to start the update data procedure in the datanodes
    // requestId used to know who to answer the to
    public static class AskUpdateData implements Serializable {
        public final Integer key;
        public final String value;
        public final String requestId;
        public AskUpdateData(Integer key, String value, String requestId) {
            this.key = key; this.value = value; this.requestId = requestId;
        }
    }

    // sended by the coordinator and received by the proper datanodes
    // used to request to read the version of the specified data to the proper datanodes
    public static class AskVersion implements Serializable {
        public final Integer key;
        public final String requestId;
        public AskVersion(Integer key, String requestId) {
            this.key = key; this.requestId = requestId;
        }
    }

    // sended by the datanodes and received by the coordinator
    // used to send the readed version of the specified data to the coordinator
    public static class SendVersion implements Serializable {
        public final Integer version;
        public final String requestId;
        public SendVersion(Integer version, String requestId) {
            this.version = version; this.requestId = requestId;
        }
    }

    // sended by the coordinator and received by the coordinator
    // used to set a timout on a reading procedure
    public static class TimeoutW implements Serializable {
        public final String requestId;
        public TimeoutW(String requestId) {
            this.requestId = requestId;
        }
    }

    // sended by the coordinator and received by the client
    // used to tell the client that a timeout error occurred on the specified
    // reading request
    public static class SendTimeoutW2Client implements Serializable {
        public final String requestId;
        public SendTimeoutW2Client(String requestId) {
            this.requestId = requestId;
        }
    }

    // sended by the coordinator to the client
    // used to send the version of the updated data to the client that requested it
    public static class SendUpdate2Client implements Serializable {
        public final Integer version;
        public final String requestId;
        public SendUpdate2Client(Integer version, String requestId) {
            this.version = version; this.requestId = requestId;
        }
    }

    // sended by the coordinator to the proper datanodes
    // used to tell to the proper datanode to update the specified data
    public static class UpdateData implements Serializable {
        public final Integer key;
        public final String value;
        public final Integer version;
        public UpdateData(Integer key, String value, Integer version) {
            this.key = key; this.value = value; this.version = version;
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
        rManager.createR(msg.requestId, getSender());
        for (ActorRef node : groupM.findDataNodes(msg.key)) {
            ReadData request = new ReadData(msg.key, msg.requestId);
            node.tell(request, self());
        }
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

                // tell all datanodes to write the updated data
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
            .build();
    }
}
