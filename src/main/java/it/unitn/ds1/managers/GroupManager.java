package it.unitn.ds1.managers;

import java.util.*;

import akka.actor.ActorRef;

import java.util.stream.Collectors;

/**
 * GroupManager
 * This class is used to manage the group of data nodes.
 * It is used by the client to find the data nodes that are responsible for a given key.
 * It is used by the data nodes to find the neighbors.
 * It is used by the join manager to find the neighbors.
 */
public class GroupManager {
    private final List<DataNodeRef> group; // must be always sorted
    public final int replicasCount;

    public GroupManager(int replicasCount) {
        this.group = new ArrayList<>();
        this.replicasCount = replicasCount;
    }

    /**
     * DataNodeRef
     * This class is used to store the reference to a data node.
     */
    static public class DataNodeRef {
        private final Integer nodeKey;
        private final ActorRef nodeRef;

        public DataNodeRef(Integer nodeKey, ActorRef nodeRef) {
            this.nodeKey = nodeKey;
            this.nodeRef = nodeRef;
        }

        /**
         * Get the key of the data node.
         * @return the key of the data node.
         */
        public Integer getNodeKey() {
            return nodeKey;
        }

        /**
         * Get the reference to the data node.
         * @return the reference to the data node.
         */
        public ActorRef getActorRef() {
            return nodeRef;
        }
    }

    /**
     * Get the index of the next data node.
     * @param i the index of the current data node.
     * @return the index of the next data node.
     */
    private int nextIndex(int i) {
        return (i + 1) % group.size();
    }

    /**
     * Get the index of the previous data node.
     * @param i the index of the current data node.
     * @return the index of the previous data node.
     */
    private int previousIndex(int i) {
        if (i == 0) {
            return group.size() - 1;
        }
        return i - 1;
    }

    /**
     * Add new data nodes to the group.
     * @param nodes the list of data nodes to add.
     */
    public void addNode(List<DataNodeRef> nodes) {
        this.group.addAll(nodes);
        this.group.sort(Comparator.comparing(DataNodeRef::getNodeKey));
    }

    /**
     * Add a new data node to the group.
     * @param node the data node to add.
     */
    public void addNode(DataNodeRef node) {
        int i = getIndex(node.getNodeKey());
        group.add(i, node);
    }

    /**
     * Find the data nodes that are responsible for a given key.
     * @param dataKey the key of the data.
     * @return the list of data nodes that are responsible for the given key.
     */
    public List<ActorRef> findDataNodes(Integer dataKey) {
        List<ActorRef> dataNodes = new ArrayList<>();
        int i = getIndex(dataKey);
        for (int j = 0; j < replicasCount; j++) {
            dataNodes.add(this.group.get(i).getActorRef());
            i = nextIndex(i);
        }
        return dataNodes;
    }

    /**
     * Find the neighbors of a given data node.
     * @param dataKey the key of the data node.
     * @return the neighbors' list of the given data node.
     */
    public List<ActorRef> findNeighbors(Integer dataKey) {
        List<ActorRef> dataNodes = new ArrayList<>();
        int idx = getIndex(dataKey);
        int i = idx;
        for (int j = 0; j < replicasCount; j++) {
            dataNodes.add(this.group.get(i).getActorRef());
            i = nextIndex(i);
        }
        i = previousIndex(idx);
        for (int j = 0; j < replicasCount; j++) {
            dataNodes.add(this.group.get(i).getActorRef());
            i = previousIndex(i);
        }
        return dataNodes;
    }

    /**
     * Remove a data node from the group.
     * @param nodeRef the reference to the data node to remove.
     */
    public void removeNode(ActorRef nodeRef) {
        group.removeIf(dataNode -> dataNode.getActorRef() == nodeRef);
    }

    /**
     * Replace the current group with a new one.
     * @param newGroup the new group of data nodes.
     */
    public void addNewGroup(List<DataNodeRef> newGroup) {
        group.clear();
        addNode(newGroup);
    }

    /**
     * Get the group of data nodes.
     * @return the group of data nodes.
     */
    public List<DataNodeRef> getGroup() {
        return group;
    }

    /**
     * Get the reference to the data nodes.
     * @return the reference to the data nodes.
     */
    public List<ActorRef> getGroupActorRef() {
        return group.stream().map(DataNodeRef::getActorRef).collect(Collectors.toList());
    }

    /**
     * Get the reference of a clockwise neighbor for a given dataKey.
     * @param dataKey the key of the data.
     * @return the reference of a clockwise neighbor for the given dataKey.
     */
    public ActorRef getClockwiseNeighbor(Integer dataKey) {
        return this.group.get(getIndex(dataKey + 1)).getActorRef();
    }

    /**
     * Find the index of the first datanode in the group responsible for the specified dataKey
     * @param dataKey the key of the data.
     * @return the index of the first responsible dataNode
     */
    private int getIndex(Integer dataKey) {
        int i = 0;
        // it is possible to use faster implementations (quicksort)
        if (this.group.get(this.group.size() - 1).getNodeKey() > dataKey)
            for (; this.group.get(i).getNodeKey() < dataKey; i++) {
            }
        return i;
    }
}
