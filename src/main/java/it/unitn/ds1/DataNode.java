package it.unitn.ds1;
import akka.actor.*;
import akka.japi.Pair;
import akka.japi.pf.ReceiveBuilder;

import java.io.Serializable;
import java.util.*;

public class DataNode extends AbstractActor {
    private final int W_quorum;
    private final int R_quorum;
    private final int timeout; // in ms
    private final int nodeKey; // Node key
    private final Map<Integer, Pair<String, Integer>> data; // key - value - version
    private final Set<ActorRef> group;

    public DataNode(int nodeKey) {

    }

    // Start message that informs every participant about its peers
    public static class JoinGroupMsg implements Serializable {
        public final List<ActorRef> group;   // an array of group members
        public JoinGroupMsg(List<ActorRef> group) {
            this.group = Collections.unmodifiableList(new ArrayList<>(group));
        }
    }

    @Override
    public Receive createReceive() {
        //return ReceiveBuilder()
        //        .build();
    }
}
