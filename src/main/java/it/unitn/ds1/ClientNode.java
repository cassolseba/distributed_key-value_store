package it.unitn.ds1;
import java.io.Serializable;

import akka.actor.*;
import it.unitn.ds1.DataNode.*;

public class ClientNode extends AbstractActor {
    // used to identify a message
    private Integer Id = 0;

    public ClientNode() {}

    static public Props props() {
        return Props.create(ClientNode.class, () -> new ClientNode());
    }

    ////////////
    // MESSAGES
    ///////////

    // tell the client to start the write procedure
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

    // tell the cilent to start the update procedure
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
    }

    public void onClientRead(ClientRead msg) {
        String requestId = self().path() + this.Id.toString();
        AskReadData data = new AskReadData(msg.key, requestId);
        System.out.println("Client " + self().path() + ", create read request[" +
                requestId + "]" + " for key:" + msg.key);
        msg.coordinator.tell(data, self());
    }

    public void onSendRead2Client(SendRead2Client msg) {
        System.out.println("Client " + self().path() + " received value: " + msg.value);
    }

    public void onClientUpdate(ClientUpdate msg) {
        String requestId = self().path() + this.Id.toString();
        AskUpdateData data = new AskUpdateData(msg.key, msg.value, requestId);
        System.out.println("Client " + self().path() + ", create update request[" +
                requestId + "]" + " for key:" + msg.key + " with value:" + msg.value);
        msg.coordinator.tell(data, self());
    }

    public void onSendUpdate2Client(SendUpdate2Client msg) {
        System.out.println("Client " + self().path() + " received version: " + msg.version);
    }

    public void onSendTimeoutR2Client(SendTimeoutR2Client msg) {
        System.out.println("Client " + self().path() + " timeout on " + msg.requestId + " reading request");
    }

    public void onSendTimeoutW2Client(SendTimeoutW2Client msg) {
        System.out.println("Client " + self().path() + " timeout on " + msg.requestId + " reading request");
    }

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
            .build();
    }
}
