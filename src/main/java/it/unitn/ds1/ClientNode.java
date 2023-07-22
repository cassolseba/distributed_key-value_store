package it.unitn.ds1;
import java.io.Serializable;

import akka.actor.*;
import it.unitn.ds1.DataNode.*;
import it.unitn.ds1.logger.Logs;

/**
 * ClientNode
 * A class that extends AbstractActor.
 * Defines the client node behavior.
 */
public class ClientNode extends AbstractActor {
    // used to identify a message
    private final Integer Id = 0;

    public ClientNode() {
        System.out.println("CLIENT: is " + self().path().name());
    }

    static public Props props() {
        return Props.create(ClientNode.class, () -> new ClientNode());
    }

    ////////////
    // MESSAGES
    ///////////

    // tell the client to start write procedure
    public static class ClientWrite implements Serializable {
        public final Integer key;
        public final String value;
        public final ActorRef coordinator;
        public ClientWrite(Integer key, String value, ActorRef coordinator) {
            this.key = key; this.value = value;
            this.coordinator = coordinator;
        }
    }

    // tell the client to start the read procedure
    public static class ClientRead implements Serializable {
        public final Integer key;
        public final ActorRef coordinator;
        public ClientRead(Integer key, ActorRef coordinator) {
            this.key = key;
            this.coordinator = coordinator;
        }
    }

    // tell the client to start the update procedure
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

    ////////////
    // HANDLERS
    ////////////

    public void onClientWrite(ClientWrite msg) {
        AskWriteData data = new AskWriteData(msg.key, msg.value);
        msg.coordinator.tell(data, self());

        // logging
        Logs.client_write(msg.key, msg.value, self().path().name(), msg.coordinator.path().name());
    }

    public void onClientRead(ClientRead msg) {
        String requestId = self().path() + this.Id.toString();
        AskReadData data = new AskReadData(msg.key, requestId);
        // System.out.println("Client " + self().path() + ",
        // create read request["requestId + "]" + " for key:" + msg.key);
        msg.coordinator.tell(data, self());

        // logging
        Logs.client_read(msg.key, self().path().name(), msg.coordinator.path().name());
    }

    public void onSendRead2Client(SendRead2Client msg) {
        // System.out.println("Client " + self().path() + " received value: " + msg.value);

        // logging
        Logs.read_reply_on_client(msg.value, msg.requestId, getSender().path().name(), self().path().name());
    }

    public void onClientUpdate(ClientUpdate msg) {
        String requestId = self().path() + this.Id.toString();
        AskUpdateData data = new AskUpdateData(msg.key, msg.value, requestId);
        //System.out.println("Client " + self().path() + ", create update request["
        // + requestId + "]" + " for key:" + msg.key + " with value:" + msg.value);
        msg.coordinator.tell(data, self());

        // logging
        Logs.client_update(msg.key, msg.value, self().path().name(), msg.coordinator.path().name());
    }

    public void onSendUpdate2Client(SendUpdate2Client msg) {
        // logging
        Logs.update_reply_on_client(msg.version, msg.requestId, getSender().path().name(), self().path().name());
        //System.out.println("Client " + self().path() + " received version: " + msg.version);
    }

    public void onSendTimeoutR2Client(SendTimeoutR2Client msg) {
        System.out.println("Client " + self().path() + " timeout on " + msg.requestId + " reading request");
    }

    public void onSendTimeoutW2Client(SendTimeoutW2Client msg) {
        System.out.println("Client " + self().path() + " timeout on " + msg.requestId + " reading request");
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
            .match(SendUpdate2Client.class, this::onSendUpdate2Client)
            .match(SendTimeoutR2Client.class, this::onSendTimeoutR2Client)
            .match(SendTimeoutW2Client.class, this::onSendTimeoutW2Client)
                .match(StatusRequest.class, this::onStatusRequest) // ----- DEBUG -------
            .build();
    }
}
