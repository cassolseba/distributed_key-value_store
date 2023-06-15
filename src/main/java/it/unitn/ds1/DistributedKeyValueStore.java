package it.unitn.ds1;

import akka.actor.Actor;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.japi.Pair;

import java.util.*;
import java.io.IOException;

import it.unitn.ds1.DataNode.AskWriteData;
import it.unitn.ds1.DataNode.InitializeDataGroup;

public class DistributedKeyValueStore {
    public static void main(String[] args) {
        final int N_dataNode = 10;
        final int N_dataElem = 20;

        final int N_replica = 3;
        final int W_quorum = 6;
        final int R_quorum = 6;
        final int maxTimeout = 1000;

        final ActorSystem system = ActorSystem.create("DKVSsystem");

        // create group
        List<Pair<Integer, ActorRef>> group = new ArrayList<>();
        Random rand = new Random(1337);
        int nodeKey = 1;
        int maxNodeKey = 1;
        for (int i=0; i<N_dataNode; i++) {
            ActorRef actorRef = system.actorOf(DataNode.props(W_quorum, R_quorum, N_replica, maxTimeout, nodeKey), "datanode"+i);
            group.add(new Pair<Integer, ActorRef>(nodeKey, actorRef));

            nodeKey += rand.nextInt(8, 15);
            maxNodeKey = nodeKey;
        }
        Collections.sort(group, Comparator.comparing(p -> p.first()));

        // send initilization to datanodes
        InitializeDataGroup initMsg = new InitializeDataGroup(group);
        for (Pair<Integer, ActorRef> elem: group) {
           elem.second().tell(initMsg, ActorRef.noSender());
        }

        inputContinue();

        AskWriteData test = new AskWriteData(80, "CIAO");
        ActorRef coordinator = group.get(0).second();
        coordinator.tell(test, ActorRef.noSender());

        inputContinue();

        for (int i=0; i<N_dataElem; i++) {
            Integer dataKey = rand.nextInt(1, maxNodeKey);
            AskWriteData msg = new AskWriteData(dataKey, dataKey.toString());
            coordinator.tell(msg, ActorRef.noSender());
        }

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
