package it.unitn.ds1;
import java.util.*;
import akka.japi.Pair;
import akka.actor.Actor;
import akka.actor.ActorRef;
import java.util.stream.Collectors;

public class GroupManager {
    private final List<DataNodeRef> group; // must be always sorted
    private int size;
    public final int N_replica;

    public GroupManager(int N_replica) {
        this.group = new ArrayList<>();
        this.size = 0;
        this.N_replica = N_replica;
    }

    static public class DataNodeRef {
        private Integer nodeKey;
        private ActorRef node;

        public DataNodeRef(Integer nodeKey, ActorRef node) {
            this.nodeKey = nodeKey; this.node = node;
        }
        public Integer getNodeKey() { return nodeKey; }
        public ActorRef getActorRef() { return node; }
    }

    public void add(List<DataNodeRef> nodes) {
        this.group.addAll(nodes);
        Collections.sort(this.group, Comparator.comparing(p -> p.getNodeKey()));
        this.size = group.size();
    }

    public void add(DataNodeRef node) {
        int i = getIdx(node.getNodeKey());
        group.add(i, node);
        this.size = group.size();
    }

    public List<ActorRef> findDataNodes(Integer dataKey) {
        List<ActorRef> dataNodes = new ArrayList<>();
        int i = getIdx(dataKey);
        for (int j=0; j<N_replica; j++) {
            dataNodes.add(this.group.get(i).getActorRef());
            i = (i+1) % size;
        }
        return dataNodes;
    }

    public List<DataNodeRef> getGroup() {
        return group;
    }

    public List<ActorRef> getGroupActorRef() {
        return group.stream().map( elem -> elem.getActorRef() ).collect(Collectors.toList());
    }

    public ActorRef getClockwiseNeighbor(Integer dataKey) {
        return this.group.get(getIdx(dataKey + 1)).getActorRef();
    }

    private int getIdx(Integer dataKey) {
        int i=0;
        // is possible use faster implementations (quicksort)
        if (this.group.get(this.group.size()-1).getNodeKey() > dataKey)
            for (;this.group.get(i).getNodeKey()<dataKey; i++) {}
        return i;
    }
}
