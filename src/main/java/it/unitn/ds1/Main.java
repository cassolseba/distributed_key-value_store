/**
 * DISTRIBUTED KEY-VALUE STORE
 * Distributed Systems 1
 * University of Trento
 *
 * @author Samuele Angheben
 * @author Sebastiano Cassol
 */

package it.unitn.ds1;

import akka.actor.ActorRef;
import it.unitn.ds1.database.DistributedKeyValueStore;

import java.io.IOException;

public class Main {

    private static final int N = 3;
    private static final int W = 2;
    private static final int R = 2;
    private static final int T = 1000;
    private static final int dataNodeCount = 5;
    private static final int dataCount = 10;
    private static final int clientCount = 3;

    public static void main(String[] args) {
        DistributedKeyValueStore database = new DistributedKeyValueStore(
                "DKVSystem", N, W, R, dataNodeCount, clientCount);

        ActorRef firstClient = database.getClient(0);
        ActorRef secondClient = database.getClient(1);
        ActorRef thirdClient = database.getClient(2);

        ActorRef firstDataNode = database.getDataNode(0);
        ActorRef secondDataNode = database.getDataNode(1);
        ActorRef thirdDataNode = database.getDataNode(2);
        ActorRef fourthDataNode = database.getDataNode(3);
        ActorRef fifthDataNode = database.getDataNode(4);

        inputContinue();

        // insert random data in the distributed database
        // database.initRandomData(dataCount);

        // insert some data in the distributed database
        database.sendWriteFromClient(firstClient, firstDataNode, 5, "FIVE");
        database.sendWriteFromClient(firstClient, firstDataNode, 17, "SEVENTEEN");
        database.sendWriteFromClient(firstClient, firstDataNode, 29, "TWENTY-NINE");
        database.sendWriteFromClient(firstClient, firstDataNode, 43, "FORTY-THREE");

        inputContinue();

        // print the status of the data nodes
        database.statusMessage(firstClient);

        inputContinue();

        // read a key from the database
        database.sendReadFromClient(firstClient, firstDataNode, 17);

        inputContinue();

        // update a value in the database
        database.sendUpdateFromClient(firstClient, thirdDataNode, 17, "SE-VEN-TEEN");

        inputContinue();

        database.statusMessage(firstClient);

        inputContinue();

        database.sendReadFromClient(firstClient, firstDataNode, 17);

        inputContinue();

        // create a new data node and join it to the group
        ActorRef joiner = database.createDataNode("JOIN1", 45, W, R, N, T);
        ActorRef bootstrapper = database.getRandomDataNode();
        database.join(joiner, bootstrapper);

        inputContinue();

        database.statusMessage(firstClient);

        inputContinue();

        // pick a random data node and make it leave
        ActorRef leaver = database.getRandomDataNode();
        database.leave(leaver);
        // need to remove this key from the group

        inputContinue();

        database.statusMessage(firstClient);

        inputContinue();

        // pick a random data node and make it crash
        ActorRef crasher = database.getRandomDataNode();
        database.crash(crasher);

        inputContinue();

        // pick a random data node that will help the crashed node to recover
        bootstrapper = database.getRandomDataNode();
        database.recover(crasher, bootstrapper);

        inputContinue();

        System.exit(0);
    }

    public static void inputContinue() {
        try {
            System.out.println("-- ENTER to continue --");
            System.in.read();
        } catch (IOException ignored) {}
    }
}
