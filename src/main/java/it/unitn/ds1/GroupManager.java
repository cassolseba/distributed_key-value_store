package it.unitn.ds1;

import java.util.*;

import akka.actor.ActorRef;

import java.util.stream.Collectors;

public class GroupManager {
    private final List<DataNodeRef> group; // must be always sorted
    public final int replicas;

    public GroupManager(int replicas) {
        this.group = new ArrayList<>();
        this.replicas = replicas;
    }

    static public class DataNodeRef {
        private final Integer nodeKey;
        private final ActorRef nodeRef;

        public DataNodeRef(Integer nodeKey, ActorRef nodeRef) {
            this.nodeKey = nodeKey;
            this.nodeRef = nodeRef;
        }

        public Integer getNodeKey() {
            return nodeKey;
        }

        public ActorRef getActorRef() {
            return nodeRef;
        }
    }

    private int nextIndex(int i) {
        return (i + 1) % group.size();
    }

    private int previousIndex(int i) {
        if (i == 0) {
            return group.size() - 1;
        }
        return i - 1;
    }

    public void addNode(List<DataNodeRef> nodes) {
        this.group.addAll(nodes);
        this.group.sort(Comparator.comparing(DataNodeRef::getNodeKey));
    }

    public void addNode(DataNodeRef node) {
        int i = getIndex(node.getNodeKey());
        group.add(i, node);
    }

    public List<ActorRef> findDataNodes(Integer dataKey) {
        List<ActorRef> dataNodes = new ArrayList<>();
        int i = getIndex(dataKey);
        for (int j = 0; j < replicas; j++) {
            dataNodes.add(this.group.get(i).getActorRef());
            i = nextIndex(i);
        }
        return dataNodes;
    }

    public List<ActorRef> findNeighbors(Integer dataKey) {
        List<ActorRef> dataNodes = new ArrayList<>();
        int idx = getIndex(dataKey);
        int i = idx;
        for (int j = 0; j < replicas; j++) {
            dataNodes.add(this.group.get(i).getActorRef());
            i = nextIndex(i);
        }
        i = previousIndex(idx);
        for (int j = 0; j < replicas; j++) {
            dataNodes.add(this.group.get(i).getActorRef());
            i = previousIndex(i);
        }
        return dataNodes;
    }

    public void removeNode(ActorRef nodeRef) {
        group.removeIf(dataNode -> dataNode.getActorRef() == nodeRef);
    }

    public void addNewGroup(List<DataNodeRef> newGroup) {
        group.clear();
        addNode(newGroup);
    }

    public List<DataNodeRef> getGroup() {
        return group;
    }

    public List<ActorRef> getGroupActorRef() {
        return group.stream().map(DataNodeRef::getActorRef).collect(Collectors.toList());
    }

    public ActorRef getClockwiseNeighbor(Integer dataKey) {
        return this.group.get(getIndex(dataKey + 1)).getActorRef();
    }

    private int getIndex(Integer dataKey) {
        int i = 0;
        // it is possible to use faster implementations (quicksort)
        if (this.group.get(this.group.size() - 1).getNodeKey() > dataKey)
            for (; this.group.get(i).getNodeKey() < dataKey; i++) {
            }
        return i;
    }
}
