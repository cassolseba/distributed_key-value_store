package it.unitn.ds1.database;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;

import java.util.*;
import it.unitn.ds1.actors.ClientNode;
import it.unitn.ds1.actors.ClientNode.*;
import it.unitn.ds1.actors.DataNode;
import it.unitn.ds1.actors.DataNode.InitializeDataGroup;
import it.unitn.ds1.actors.DataNode.AskToJoin;
import it.unitn.ds1.actors.DataNode.AskToLeave;
import it.unitn.ds1.actors.DataNode.AskCrash;
import it.unitn.ds1.actors.DataNode.AskRecover;
import it.unitn.ds1.managers.GroupManager.DataNodeRef;

/**
 * DistributedKeyValueStore
 * Defines the actor system and the main methods to interact with it.
 */
public class DistributedKeyValueStore {
    private final int N; // number of replicas
    private final int W; // write quorum
    private final int R; // read quorum
    private final int T; // max timeout
    private final ActorSystem actorSystem;
    private final List<DataNodeRef> dataNodes;
    private final List<ActorRef> clients;

    /**
     * DistributedKeyValueStore
     * Constructor with fixed data nodes and clients, useful for testing
     * @param systemName name of the actor system
     * @param N number of replicas
     * @param W write quorum
     * @param R read quorum
     * @param T timeout
     */
    public DistributedKeyValueStore(String systemName, int N, int W, int R, int T) {
        this.N = N;
        this.W = W;
        this.R = R;
        this.T = T;

        if (W > N || R > N) {
            System.out.println("ERROR: W or R are greater than N");
            System.exit(0);
        } else if (N > 10) {
            System.out.println("ERROR: N is greater than the number of data nodes");
            System.exit(0);
        } else if (R + W <= N || W <= roundUp(N,2)) {
            System.out.println("ERROR: Constrains not satisfied");
            System.exit(0);
        }

        if (T < 1000) {
            System.out.println("ERROR: T should be at least 1000ms");
            System.exit(0);
        }

        this.actorSystem = ActorSystem.create(systemName);
        this.dataNodes = new ArrayList<DataNodeRef>();
        this.clients = new ArrayList<ActorRef>();

        // Manually add some data nodes
        ActorRef dataNode1 = createDataNode("DATA1", 10);
        this.dataNodes.add(new DataNodeRef(10, dataNode1));
        ActorRef dataNode2 = createDataNode("DATA2", 20);
        this.dataNodes.add(new DataNodeRef(20, dataNode2));
        ActorRef dataNode3 = createDataNode("DATA3", 30);
        this.dataNodes.add(new DataNodeRef(30, dataNode3));
        ActorRef dataNode4 = createDataNode("DATA4", 40);
        this.dataNodes.add(new DataNodeRef(40, dataNode4));
        ActorRef dataNode5 = createDataNode("DATA5", 50);
        this.dataNodes.add(new DataNodeRef(50, dataNode5));
        ActorRef dataNode6 = createDataNode("DATA6", 60);
        this.dataNodes.add(new DataNodeRef(60, dataNode6));
        ActorRef dataNode7 = createDataNode("DATA7", 70);
        this.dataNodes.add(new DataNodeRef(70, dataNode7));
        ActorRef dataNode8 = createDataNode("DATA8", 80);
        this.dataNodes.add(new DataNodeRef(80, dataNode8));
        ActorRef dataNode9 = createDataNode("DATA9", 90);
        this.dataNodes.add(new DataNodeRef(90, dataNode9));
        ActorRef dataNode10 = createDataNode("DATA10", 100);
        this.dataNodes.add(new DataNodeRef(100, dataNode10));

        // Manually add some clients
        ActorRef client1 = createClientNode("CLIENT1");
        this.clients.add(client1);
        ActorRef client2 = createClientNode("CLIENT2");
        this.clients.add(client2);

        connectDataNodes();
    }

    /**
     * DistributedKeyValueStore
     * Constructor with variable data nodes and clients, defined in class Main
     * @param systemName name of the actor system
     * @param N number of replicas
     * @param W write quorum
     * @param R read quorum
     * @param T timeout
     * @param dataNodeCount number of data nodes
     * @param clientCount number of clients
     */
    public DistributedKeyValueStore(String systemName, int N, int W, int R, int T, int dataNodeCount, int clientCount) {
        this.N = N;
        this.W = W;
        this.R = R;
        this.T = T;

        if (W > N || R > N) {
            System.out.println("ERROR: W or R are greater than N");
            System.exit(0);
        } else if (N > dataNodeCount) {
            System.out.println("ERROR: N is greater than the number of data nodes");
            System.exit(0);
        } else if (R + W <= N || W <= roundUp(N,2)) {
            System.out.println("ERROR: Constrains not satisfied");
            System.exit(0);
        }

        if (T < 1000) {
            System.out.println("ERROR: T should be at least 1000ms");
            System.exit(0);
        }

        this.actorSystem = ActorSystem.create(systemName);
        this.dataNodes = initDataNodes(dataNodeCount);
        this.clients = initClients(clientCount);

        connectDataNodes();
    }

    /**
     * initDataNodes
     * Initialize data nodes with keys 10, 20, 30, 40, 50, ... and name DATA1, DATA2, DATA3, ...
     * @param dataNodesCount number of data nodes
     * @return a list of data nodes
     */
    private List<DataNodeRef> initDataNodes(int dataNodesCount) {
        List<DataNodeRef> dataNodes = new ArrayList<>();

        int key = 10;
        for (int i = 0; i < dataNodesCount; i++) {
            ActorRef dataNode = createDataNode("DATA" + (i + 1), key);
            dataNodes.add(new DataNodeRef(key, dataNode));
            key += 10;
        }

        return dataNodes;
    }

    /**
     * initClients
     * Initialize clients with names CLIENT1, CLIENT2, CLIENT3, ...
     * @param clientCount number of clients
     * @return a list of clients
     */
    private List<ActorRef> initClients(int clientCount) {
        List<ActorRef> clients = new ArrayList<>();
        for (int i = 0; i < clientCount; i++) {
            ActorRef client = createClientNode("CLIENT" + (i + 1));
            clients.add(client);
        }
        return clients;
    }

    /**
     * createDataNode
     * Create a new data node
     * @param name name of the new data node
     * @param key key of the new data node
     * @return the actor reference of the new data node
     */
    public ActorRef createDataNode(String name, int key) {
        return actorSystem.actorOf(DataNode.props(W, R, N, T, key), name);
    }

    /**
     * createClientNode
     * Create a new client
     * @param name the name of the new client
     * @return the actor reference of the new client
     */
    public ActorRef createClientNode(String name) {
        return actorSystem.actorOf(ClientNode.props(), name);
    }

    /**
     * connectDataNodes
     * Send the group of data nodes to all the nodes in the system
     */
    private void connectDataNodes() {
        for (DataNodeRef dataNode : this.dataNodes) {
            dataNode.getActorRef().tell(new InitializeDataGroup(dataNodes), ActorRef.noSender());
        }
    }

    /**
     * writeData
     * Write a new key-value pair in the distributed database, useful for testing and initializing the system
     * @param key the new key
     * @param value the new value
     */
    private void writeData(int key, String value) {
        ActorRef coordinator = getRandomDataNode();
        ActorRef client = getRandomClient();
        sendWriteFromClient(client, coordinator, key, value);
    }

    /**
     * initRandomData
     * Write in the database random key-value pairs, useful for testing and initializing the system
     * @param dataCount number of key-value pairs to write
     */
    public void initRandomData(int dataCount) {
        Random rand = new Random();
        for (int i = 0; i < dataCount; i++) {
            int key = rand.nextInt(1, getMaxKey() + 10);
            ActorRef coordinator = getRandomDataNode();
            ActorRef client = getRandomClient();
            sendWriteFromClient(client, coordinator, key, "VALUE" + key);
        }
    }

    /**
     * getRandomDataNode
     * Get a random data node from the system
     * @return an actor reference to random data node
     */
    public ActorRef getRandomDataNode() {
        Random rand = new Random();
        return this.dataNodes.get(rand.nextInt(dataNodes.size())).getActorRef();
    }

    /**
     * getRandomClient
     * Get a random client from the system
     * @return an actor reference to a random client
     */
    public ActorRef getRandomClient() {
        Random rand = new Random();
        return this.clients.get(rand.nextInt(clients.size()));
    }

    /**
     * getMaxKey
     * Get the higher node key in the database
     * @return the higher node key
     */
    private int getMaxKey() {
        int maxKey = 0;
        for (DataNodeRef dataNode : this.dataNodes) {
            if (dataNode.getNodeKey() > maxKey) {
                maxKey = dataNode.getNodeKey();
            }
        }
        return maxKey;
    }

    // ----- Messages ----- //

    /**
     * sendWriteFromClient
     * Send a Write request to the database
     * @param client the client that sends the request
     * @param coordinator the coordinator of the request
     * @param key the key to write
     * @param value the value to write
     */
    public void sendWriteFromClient(ActorRef client, ActorRef coordinator, int key, String value) {
        ClientWrite msg = new ClientWrite(key, value, coordinator);
        client.tell(msg, ActorRef.noSender());
    }

    /**
     * sendReadFromClient
     * Send a Read request to the database
     * @param client the client that sends the request
     * @param coordinator the coordinator of the request
     * @param key the key to read
     */
    public void sendReadFromClient(ActorRef client, ActorRef coordinator, int key) {
        ClientRead msg = new ClientRead(key, coordinator);
        client.tell(msg, ActorRef.noSender());
    }

    /**
     * sendUpdateFromClient
     * Send an Update request to the database
     * @param client the client that sends the request
     * @param coordinator the coordinator of the request
     * @param key the key to update
     * @param newValue the new value to write
     */
    public void sendUpdateFromClient(ActorRef client, ActorRef coordinator, int key, String newValue) {
        ClientUpdate msg = new ClientUpdate(key, newValue, coordinator);
        client.tell(msg, ActorRef.noSender());
    }

    /**
     * join
     * Send a join request to the system
     * @param joiningNode the node that wants to join
     * @param bootstrappingNode the node that helps the joining node
     */
    public void join(ActorRef joiningNode, ActorRef bootstrappingNode) {
        AskToJoin msg = new AskToJoin(bootstrappingNode);
        joiningNode.tell(msg, ActorRef.noSender());
    }

    /**
     * leave
     * Send a leave request to the system
     * @param leavingNode the node that wants to leave
     */
    public void leave(ActorRef leavingNode) {
        AskToLeave msg = new AskToLeave();
        leavingNode.tell(msg, ActorRef.noSender());
    }

    /**
     * crash
     * Tell a node to crash
     * @param crashingNode the node that crashes
     */
    public void crash(ActorRef crashingNode) {
        AskCrash msg = new AskCrash();
        crashingNode.tell(msg, ActorRef.noSender());
    }

    /**
     * recover
     * Tell a node to recover
     * @param crashedNode the node that crashed
     * @param bootstrappingNode the node that helps the crashed node to recover
     */
    public void recover(ActorRef crashedNode, ActorRef bootstrappingNode) {
        AskRecover msg = new AskRecover(bootstrappingNode);
        crashedNode.tell(msg, ActorRef.noSender());
    }

    /**
     * statusMessage
     * Send a status request to the system
     * @param client the client that sends the request
     */
    public void statusMessage(ActorRef client) {
        StatusRequest msg = new StatusRequest(getRandomDataNode());
        client.tell(msg, ActorRef.noSender());
    }

    /**
     * getDataNode
     * Get the data node stored at index i
     * @param i the index
     * @return the ActorRef of the data node
     */
    public ActorRef getDataNode(int i) {
        return this.dataNodes.get(i).getActorRef();
    }

    /**
     * getClient
     * get the client stored at index i
     * @param i the index
     * @return the ActorRef of the client
     */
    public ActorRef getClient(int i) {
        return this.clients.get(i);
    }


    /**
     * roundUp
     * round up integer division both parameters must be positive
     * @param num the number, divisor the divisor
     * @return rounded up division
     */
    private static long roundUp(long num, long divisor) {
        return (num + divisor - 1) / divisor;
    }

}
