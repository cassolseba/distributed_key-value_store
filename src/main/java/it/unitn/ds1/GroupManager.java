package it.unitn.ds1;
import java.util.*;
import akka.japi.Pair;
import akka.actor.Actor;
import akka.actor.ActorRef;
import java.util.stream.Collectors;

public class GroupManager {
    private final List<DataNodeRef> group; // must be always sorted
    public final int N_replica;

    public GroupManager(int N_replica) {
        this.group = new ArrayList<>();
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

    private int next(int i) {
        return (i+1) % group.size();
    }

    private int prev(int i) {
        if (i == 0) {
            return group.size()-1;
        }
        return i-1;
    }

    public void add(List<DataNodeRef> nodes) {
        this.group.addAll(nodes);
        Collections.sort(this.group, Comparator.comparing(p -> p.getNodeKey()));
    }

    public void add(DataNodeRef node) {
        int i = getIdx(node.getNodeKey());
        group.add(i, node);
    }

    public List<ActorRef> findDataNodes(Integer dataKey) {
        List<ActorRef> dataNodes = new ArrayList<>();
        int i = getIdx(dataKey);
        for (int j=0; j<N_replica; j++) {
            dataNodes.add(this.group.get(i).getActorRef());
            i = next(i);
        }
        return dataNodes;
    }

    public List<ActorRef> find2KNeighbors(Integer dataKey) {
        List<ActorRef> dataNodes = new ArrayList<>();
        int idx = getIdx(dataKey);
        int i = idx;
        for (int j=0; j<N_replica; j++) {
            dataNodes.add(this.group.get(i).getActorRef());
            i = next(i);
        }
        i = prev(idx);
        for (int j=0; j<N_replica; j++) {
            dataNodes.add(this.group.get(i).getActorRef());
            i = prev(i);
        }
        return dataNodes;
    }

    public void remove(ActorRef nodeRef) {
        group.removeIf(dataNode -> dataNode.getActorRef() == nodeRef);
    }

    public void addNewGroup(List<DataNodeRef> newGroup) {
        group.clear();
        add(newGroup);
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
