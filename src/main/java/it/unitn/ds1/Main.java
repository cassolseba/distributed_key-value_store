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
import it.unitn.ds1.logger.Logs;

import java.io.IOException;

public class Main {

    private static final int N = 6;
    private static final int W = 5;
    private static final int R = 3;
    private static final int T = 5000;
    private static final int dataNodeCount = 10;
    private static final int clientCount = 4;

    public static void main(String[] args) throws InterruptedException {

        /* Instantiate a new distributed key-value store */
        Logs.printStartupInfo(N, W, R, dataNodeCount, clientCount);
        DistributedKeyValueStore database = new DistributedKeyValueStore(
                "DKVSystem", N, W, R, T, dataNodeCount, clientCount);

        Thread.sleep(1000); // wait for startup

        /* Get references to the clients */
        Logs.printClientInit();
        ActorRef firstClient = database.getClient(0);
        ActorRef secondClient = database.getClient(1);
        ActorRef thirdClient = database.getClient(2);
        ActorRef fourthClient = database.getClient(3);

        Thread.sleep(1000); // wait for client references

        /* Get references to the data nodes */
        Logs.printDatanodeInit();
        ActorRef firstDataNode = database.getDataNode(0); // key: 10
        ActorRef secondDataNode = database.getDataNode(1); // key: 20
        ActorRef thirdDataNode = database.getDataNode(2); // key: 30
        ActorRef fourthDataNode = database.getDataNode(3); // key: 40
        ActorRef fifthDataNode = database.getDataNode(4); // key: 50

        Thread.sleep(2500); // wait for datanode references

        /* Insert some data in the distributed database */
        Logs.printDataInit();
        database.sendWriteFromClient(firstClient, firstDataNode, 5, "FIVE");
        database.sendWriteFromClient(secondClient, secondDataNode, 17, "SEVENTEEN");
        database.sendWriteFromClient(thirdClient, thirdDataNode, 29, "TWENTY-NINE");
        database.sendWriteFromClient(fourthClient, fourthDataNode, 43, "FORTY-THREE");

        Thread.sleep(2500); // wait for the writes to be completed

        Logs.printStartStatusCheck();
        database.statusMessage(firstClient);

        inputContinue();
        Logs.printEndStatusCheck();

        /* ----- TESTS ----- */

        /* ---- READ ---- */

        // test 1: read from a key
//        Logs.printRunTest(1, "read from a key");
//        database.sendReadFromClient(firstClient, firstDataNode, 17);

        // test 2: read from an unknown key
//        Logs.printRunTest(2, "read from an unknown key");
//        database.sendReadFromClient(firstClient, firstDataNode, 21);


        // test 3: read a key from two different clients
//        Logs.printRunTest(3, "read a key from two different clients");
//        database.sendReadFromClient(firstClient, firstDataNode, 17);
//        database.sendReadFromClient(secondClient, secondDataNode, 17);

        // test 4: read a key twice from the same client - error expected
//        Logs.printRunTest(4, "read a key twice from the same client");
//        database.sendReadFromClient(firstClient, firstDataNode, 17);
//        database.sendReadFromClient(firstClient, secondClient, 17);

        // test 5: read a key from two different clients and the same coordinator
//        Logs.printRunTest(5, "read a key from two different clients and the same coordinator");
//        database.sendReadFromClient(firstClient, firstDataNode, 17);
//        database.sendReadFromClient(secondClient, firstDataNode, 17);

        // test 6: read a key and make an involved node crash
//        Logs.printRunTest(6, "read a key and make an involved node crash");
//        database.sendReadFromClient(firstClient, firstDataNode, 43);
//        database.crash(fourthDataNode);

        /* ---- UPDATE ---- */

        // test 7: update a value in the database
//        Logs.printRunTest(7, "update a value in the database");
//        database.sendUpdateFromClient(firstClient, firstDataNode, 17, "SE-VEN-TEEN");

        // test 8: update an unknown key
//        Logs.printRunTest(8, "update an unknown key");
//        database.sendUpdateFromClient(firstClient, firstDataNode, 21, "TWENTY-ONE");

        // test 9: read a key, then write a new value and read again
//        Logs.printRunTest(9, "read a key, then write a new value and read again");
//        database.sendReadFromClient(firstClient, firstDataNode, 17);
//        Thread.sleep(1500);
//        database.sendUpdateFromClient(firstClient, firstDataNode, 17, "SE-VEN-TEEN");
//        Thread.sleep(1500);
//        database.sendReadFromClient(firstClient, firstDataNode, 17);

        // test 10: read and update the same key at the same time
//        Logs.printRunTest(10, "read and update the same key at the same time");
//        database.sendReadFromClient(firstClient, firstDataNode, 17);
//        database.sendUpdateFromClient(secondClient, secondDataNode, 17, "SE-VEN-TEEN");

        // test 11: update two times from different clients
//        Logs.printRunTest(11, "update two times from different clients and different coordinators");
//        database.sendUpdateFromClient(firstClient, firstDataNode, 17, "SE-VEN-TEEN");
//        database.sendUpdateFromClient(secondClient, secondDataNode, 17, "SE-VEN-TEEEEEN");

        // test 12: update two times from the same client
//        Logs.printRunTest(12, "update two times from the same client and the same coordinator");
//        database.sendUpdateFromClient(firstClient, firstDataNode, 17, "SE-VEN-TEEN");
//        database.sendUpdateFromClient(firstClient, secondDataNode, 17, "SE-VEN-TEEEEEN");

        // test 13: update two times from different clients and the same coordinator
//        Logs.printRunTest(13, "update two times from different clients and the same coordinator");
//        database.sendUpdateFromClient(firstClient, firstDataNode, 17, "SE-VEN-TEEN");
//        database.sendUpdateFromClient(secondClient, firstDataNode, 17, "SE-VEN-TEEEEEN");

        // test 14: update a key and make an interested node crash
//        Logs.printRunTest(14, "update a key and make an interested node crash");
//        database.sendUpdateFromClient(firstClient, firstDataNode, 17, "SE-VEN-TEEN");
//        database.crash(secondDataNode);

        // test 15: update a key two times and read from two different clients
//        Logs.printRunTest(15, "update a key two times and read from two different clients");
//        database.sendUpdateFromClient(firstClient, firstDataNode, 5, "51");
//        inputContinue();
//        database.sendUpdateFromClient(secondClient, secondDataNode, 5, "52");
//        inputContinue();
//        database.sendReadFromClient(thirdClient, thirdDataNode, 5);
//        inputContinue();
//        database.sendReadFromClient(fourthClient, fourthDataNode, 5);

        /* ---- JOIN ---- */
        // test 16: join new node
//        Logs.printRunTest(16, "join new node");
//        ActorRef newDataNode = database.createDataNode("NEW_DATA11", 110);
//        database.join(newDataNode, fifthDataNode);

        /* ---- LEAVE ---- */
        // test 17: leave
//        Logs.printRunTest(17, "leave");
//        database.leave(secondDataNode);

        /* ---- CRASH & RECOVER ---- */
        // test 18: crash a node and recover
        Logs.printRunTest(18, "crash a node and recover");
        database.crash(firstDataNode);
        inputContinue();
        database.sendUpdateFromClient(firstClient, secondDataNode, 5, "51");
        inputContinue();
        database.statusMessage(secondClient);
        inputContinue();
        database.recover(firstDataNode, secondDataNode);


        /* ---- END PHASE ---- */
        inputContinue();

        Logs.printStartStatusCheck();
        database.statusMessage(secondClient);

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
