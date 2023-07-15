package it.unitn.ds1;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;

import java.util.*;
import java.io.IOException;

import it.unitn.ds1.ClientNode.*;
import it.unitn.ds1.DataNode.InitializeDataGroup;
import it.unitn.ds1.GroupManager.DataNodeRef;

public class DistributedKeyValueStore {
    public static void main(String[] args) {
        final int N_dataNode = 10;
        final int N_dataElem = 20;
        final int N_clients = 3;

        final int N_replica = 3;
        final int W_quorum = 2;
        final int R_quorum = 2;
        final int maxTimeout = 1000;

        final ActorSystem system = ActorSystem.create("DKVSsystem");

        // create datanodes group
        List<DataNodeRef> group = new ArrayList<>();
        Random rand = new Random(1337);
        int nodeKey = 1;
        int maxNodeKey = 1;
        for (int i=0; i<N_dataNode; i++) {
            ActorRef actorRef = system.actorOf(DataNode.props(W_quorum, R_quorum, N_replica, maxTimeout, nodeKey), "datanode"+i);
            group.add(new DataNodeRef(nodeKey, actorRef));

            maxNodeKey = nodeKey;
            nodeKey += rand.nextInt(8, 15);
        }

        // send initilization to datanodes
        InitializeDataGroup initMsg = new InitializeDataGroup(group);
        for (DataNodeRef elem: group) {
           elem.getActorRef().tell(initMsg, ActorRef.noSender());
        }

        inputContinue();

        // create the clients
        List<ActorRef> clients = new ArrayList<>();
        for (int i=0; i<N_clients; i++) {
            clients.add(system.actorOf(ClientNode.props(), "clientNode"+i));
        }

        // write data into datanodes by clients
        for (int i=0; i<N_dataElem; i++) {
            // create random data
            Integer dataKey = rand.nextInt(1, maxNodeKey+5);
            // select random datanodes (coordinator)
            ActorRef coordinator = group.get(rand.nextInt(group.size())).getActorRef();
            // select random client
            ActorRef client = clients.get(rand.nextInt(clients.size()));

            ClientWrite msg = new ClientWrite(dataKey, "DATA"+dataKey.toString(), coordinator);
            client.tell(msg, ActorRef.noSender());
        }

        inputContinue();

        // try read
        ActorRef client = clients.get(0);
        client.tell(new ClientRead(95, group.get(0).getActorRef()), ActorRef.noSender());

        inputContinue();

        // try update
        client.tell(new ClientUpdate(95, "DATA95-UPDATED", group.get(0).getActorRef()), ActorRef.noSender());

        inputContinue();

        system.terminate();
    }

    public static void inputContinue() {
        try {
            System.out.println("-- ENTER to continue --");
            System.in.read();
        }
        catch (IOException ignored) {}
    }
}
