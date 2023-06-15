package it.unitn.ds1;
import akka.actor.*;
import akka.io.Tcp.Write;
import akka.japi.pf.ReceiveBuilder;
import akka.japi.Pair;

import java.io.Serializable;
import java.util.*;

import javax.xml.crypto.Data;

public class DataNode extends AbstractActor {
    private final int W_quorum;
    private final int R_quorum;
    private final int N_replica;
    private int N_dataNode;
    private final int maxTimeout; // in ms
    public final Integer nodeKey; // Node key
    private final Map<Integer, Pair<String, Integer>> dataStore; // key - value - version
    private final List<Pair<Integer, ActorRef>> group; // must be always sorted

    public DataNode(int W_quorum, int R_quorum, int N_replica, int maxTimeout, int nodeKey) {
        this.W_quorum = W_quorum;
        this.R_quorum = R_quorum;
        this.N_replica = N_replica;
        this.maxTimeout = maxTimeout;
        this.nodeKey = nodeKey;
        this.dataStore = new HashMap<>();
        this.group = new ArrayList<>();

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
        public final List<Pair<Integer, ActorRef>> group;
        public InitializeDataGroup(List<Pair<Integer, ActorRef>> group) {
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

    ////////////
    // HANDLERS
    ////////////

    public void onInitializeDataGroup(InitializeDataGroup msg) {
        this.group.addAll(msg.group);
        Collections.sort(this.group, Comparator.comparing(p -> p.first()));
        this.N_dataNode = group.size();
    }

    public void onAskWriteData(AskWriteData msg) {
        int i=0;
        if (this.group.get(this.group.size()-1).first() > msg.key)
            for (;this.group.get(i).first()<msg.key; i++) {}
        for (int j=0; j<N_replica; j++) {
            WriteData data = new WriteData(msg.key, msg.value);
            this.group.get(i).second().tell(data, self());
            i = (i+1) % N_dataNode;
        }
    }

    public void onWriteData(WriteData data) {
        dataStore.put(data.key, new Pair<String, Integer>(data.value, 1));
        System.out.println("DataNode " + self().path().name() + ": data (" + data.key + "," + data.value + ") saved");
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
            .match(InitializeDataGroup.class, this::onInitializeDataGroup)
            .match(AskWriteData.class, this::onAskWriteData)
            .match(WriteData.class, this::onWriteData)
            .build();
    }
}
