package it.unitn.ds1.actors;

import java.io.Serializable;

import akka.actor.*;
import it.unitn.ds1.actors.DataNode.*;
import it.unitn.ds1.logger.Logs;
import it.unitn.ds1.logger.TimeoutType;
import it.unitn.ds1.util.Helper;

/**
 * Client Node
 * A class that extends AbstractActor.
 * Defines the client node behavior.
 */
public class ClientNode extends AbstractActor {
    // used to identify a message
    private final Integer Id = 0;

    public ClientNode() {
        System.out.println("CLIENT: is " + Helper.getName(self()));
    }

    static public Props props() {
        return Props.create(ClientNode.class, ClientNode::new);
    }

    /* ------- MESSAGES ------- */

    /**
     * A message used to request a write operation.
     */
    public static class ClientWrite implements Serializable {
        public final Integer key;
        public final String value;
        public final ActorRef coordinator;

        public ClientWrite(Integer key, String value, ActorRef coordinator) {
            this.key = key;
            this.value = value;
            this.coordinator = coordinator;
        }
    }

    /**
     * A message used to request a read operation.
     */
    public static class ClientRead implements Serializable {
        public final Integer key;
        public final ActorRef coordinator;

        public ClientRead(Integer key, ActorRef coordinator) {
            this.key = key;
            this.coordinator = coordinator;
        }
    }

    /**
     * A message used to request an update operation.
     */
    public static class ClientUpdate implements Serializable {
        public final Integer key;
        public final String value;
        public final ActorRef coordinator;

        public ClientUpdate(Integer key, String value, ActorRef coordinator) {
            this.key = key;
            this.value = value;
            this.coordinator = coordinator;
        }
    }

    /* ------- HANDLERS ------- */

    /**
     * ClientWrite message handler.
     * @param msg is a ClientWrite message
     */
    public void onClientWrite(ClientWrite msg) {
        AskWriteData data = new AskWriteData(msg.key, msg.value);
        msg.coordinator.tell(data, self());

        // logging
        Logs.client_write(msg.key, msg.value, Helper.getName(self()), msg.coordinator.path().name());
    }

    /**
     * ClientRead message handler.
     * @param msg is a ClientRead message
     */
    public void onClientRead(ClientRead msg) {
        String requestId = self().path() + this.Id.toString();
        AskReadData data = new AskReadData(msg.key, requestId);
        // System.out.println("Client " + self().path() + ",
        // create read request["requestId + "]" + " for key:" + msg.key);
        msg.coordinator.tell(data, self());

        // logging
        Logs.client_read(msg.key, Helper.getName(self()), msg.coordinator.path().name());
    }

    /**
     * SendRead2Client message handler.
     * @param msg is a SendRead2Client message
     */
    public void onSendRead2Client(SendRead2Client msg) {
        // System.out.println("Client " + self().path() + " received value: " + msg.value);

        // logging
        Logs.read_reply_on_client(msg.value, msg.requestId, Helper.getName(getSender()), Helper.getName(self()));
    }

    /**
     * ClientUpdate message handler.
     * @param msg is a ClientUpdate message
     */
    public void onClientUpdate(ClientUpdate msg) {
        String requestId = self().path() + this.Id.toString();
        AskUpdateData data = new AskUpdateData(msg.key, msg.value, requestId);
        // System.out.println("Client " + self().path() + ", create update request["
        // + requestId + "]" + " for key:" + msg.key + " with value:" + msg.value);
        msg.coordinator.tell(data, self());

        // logging
        Logs.client_update(msg.key, msg.value, Helper.getName(self()), msg.coordinator.path().name());
    }

    /**
     * ReturnUpdate message handler.
     * Return the updated value to the client.
     * @param msg is a ReturnUpdate message
     */
    public void onReturnUpdate(ReturnUpdate msg) {
        // logging
        Logs.update_reply_on_client(msg.version, msg.requestId, Helper.getName(getSender()), Helper.getName(self()));
        //System.out.println("Client " + self().path() + " received version: " + msg.version);
    }

    /**
     * ReturnTimeoutOnRead message handler.
     * @param msg is a ReturnTimeoutOnRead message
     */
    public void onReturnTimeoutOnRead(ReturnTimeoutOnRead msg) {
        // System.out.println("Client " + self().path() + " timeout on " + msg.requestId + " reading request");

        // logging
        Logs.timeout(TimeoutType.READ, msg.requestId, Helper.getName(getSender()), Helper.getName(self()));
    }

    /**
     * ReturnTimeoutOnWrite message handler.
     * @param msg is a ReturnTimeoutOnWrite message
     */
    public void onReturnTimeoutOnWrite(ReturnTimeoutOnWrite msg) {
        // System.out.println("Client " + self().path() + " timeout on " + msg.requestId + " reading request");

        // logging
        Logs.timeout(TimeoutType.WRITE, msg.requestId, Helper.getName(getSender()), Helper.getName(self()));
    }

    // DEBUG CLASSES AND FUNCTIONS
    // __________________________________________________________________________

    /**
     * Status request message.
     * Specify a coordinator that will start the status capture operation.
     */
    public static class StatusRequest implements Serializable {
        public final ActorRef coordinator;

        public StatusRequest(ActorRef coordinator) {
            this.coordinator = coordinator;
        }
    }

    /**
     * Status request handler.
     * Send a status request to the coordinator.
     * @param msg is a StatusRequest message
     */
    public void onStatusRequest(StatusRequest msg) {
        AskStatus req = new AskStatus();
        msg.coordinator.tell(req, self());
    }

    // __________________________________________________________________________
    // END DEBUG CLASSES AND FUNCTIONS

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(ClientWrite.class, this::onClientWrite)
                .match(ClientRead.class, this::onClientRead)
                .match(SendRead2Client.class, this::onSendRead2Client)
                .match(ClientUpdate.class, this::onClientUpdate)
                .match(ReturnUpdate.class, this::onReturnUpdate)
                .match(ReturnTimeoutOnRead.class, this::onReturnTimeoutOnRead)
                .match(ReturnTimeoutOnWrite.class, this::onReturnTimeoutOnWrite)
                .match(StatusRequest.class, this::onStatusRequest) // ----- DEBUG -------
                .build();
    }
}