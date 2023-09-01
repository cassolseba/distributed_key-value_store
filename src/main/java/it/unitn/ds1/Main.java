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

    public static void main(String[] args) throws InterruptedException {
        /* Instantiate a new distributed key-value store */
        DistributedKeyValueStore database = new DistributedKeyValueStore(
                "DKVSystem", N, W, R, dataNodeCount, clientCount);

        /* Get references to the clients */
        ActorRef firstClient = database.getClient(0);
        ActorRef secondClient = database.getClient(1);
        ActorRef thirdClient = database.getClient(2);

        /* Get references to the data nodes */
        ActorRef firstDataNode = database.getDataNode(0); // key: 10
        ActorRef secondDataNode = database.getDataNode(1); // key: 20
        ActorRef thirdDataNode = database.getDataNode(2); // key: 30
        ActorRef fourthDataNode = database.getDataNode(3); // key: 40
        ActorRef fifthDataNode = database.getDataNode(4); // key: 50

        /* Wait for the system to be ready */
        Thread.sleep(2500);

        /* Insert some data in the distributed database */
        database.sendWriteFromClient(firstClient, firstDataNode, 5, "FIVE");
        database.sendWriteFromClient(firstClient, firstDataNode, 17, "SEVENTEEN");
        database.sendWriteFromClient(firstClient, firstDataNode, 29, "TWENTY-NINE");
        database.sendWriteFromClient(firstClient, firstDataNode, 43, "FORTY-THREE");

        /* Wait for the writes to be completed */
        Thread.sleep(2500);

        /* ----- TESTS ----- */

        // test 1: read from a key
        // TODO fix READ_REPLY names, it seems duplicated but it is not
//        database.sendReadFromClient(firstClient, firstDataNode, 17);

        // test 2: read form an unknown key
        // TODO handle read from unknown key
//        database.sendReadFromClient(firstClient, firstDataNode, 21);

        // test 3: update a value in the database
//        database.sendUpdateFromClient(firstClient, firstDataNode, 17, "SE-VEN-TEEN");

        // test 4: update an unknown key
        // TODO handle update from unknown key
//        database.sendUpdateFromClient(firstClient, firstDataNode, 21, "TWENTY-ONE");

        // test 5: read a key twice from the same client and the same coordinator
//        database.sendReadFromClient(firstClient, firstDataNode, 17);
//        database.sendReadFromClient(firstClient, firstDataNode, 17);

        // test 6: read a key from two different clients and two different coordinators
//        database.sendReadFromClient(firstClient, firstDataNode, 17);
//        database.sendReadFromClient(secondClient, secondDataNode, 17);

        // test 7: read a key from two different clients and the same coordinator
//        database.sendReadFromClient(firstClient, firstDataNode, 17);
//        database.sendReadFromClient(secondClient, firstDataNode, 17);





        Thread.sleep(10000);
        System.exit(0);
    }

    /*
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
    }

    public static void inputContinue() {
        try {
            System.out.println("-- ENTER to continue --");
            System.in.read();
        } catch (IOException ignored) {}
    } */
}
