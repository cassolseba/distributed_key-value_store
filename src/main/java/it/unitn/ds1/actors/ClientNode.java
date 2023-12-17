package it.unitn.ds1.actors;

import java.io.Serializable;

import akka.actor.*;
import com.sun.tools.jconsole.JConsoleContext;
import it.unitn.ds1.actors.DataNode.*;
import it.unitn.ds1.logger.ErrorType;
import it.unitn.ds1.logger.Logs;
import it.unitn.ds1.logger.TimeoutType;
import it.unitn.ds1.utils.Helper;

/**
 * Client Node
 * Actor that represents a client node in the distributed system
 */
public class ClientNode extends AbstractActor {
    // used to identify a message
    private Integer Id = 0;
    private boolean isBusy = false;

    public ClientNode() {
        System.out.println("CLIENT: is " + Helper.getName(self()));
    }

    static public Props props() {
        return Props.create(ClientNode.class, ClientNode::new);
    }

    /* ------- MESSAGES ------- */

    /**
     * ClientWrite
     * A message used to request a write operation.
     * It is sent by the client and received by the coordinator.
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
     * ClientRead
     * A message used to request a read operation.
     * It is sent by the client and received by the coordinator.
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
     * ClientUpdate
     * A message used to request an update operation.
     * It is sent by the client and received by the coordinator.
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
     * @param msg ClientWrite message
     * @see ClientWrite
     */
    public void onClientWrite(ClientWrite msg) {
        if (!this.isBusy) {
            this.isBusy = true;
            AskWriteData data = new AskWriteData(msg.key, msg.value);
            msg.coordinator.tell(data, self());

            // logging
            Logs.client_write(msg.key, msg.value, Helper.getName(self()), msg.coordinator.path().name());
            this.isBusy = false;
        } else {
            Logs.error(ErrorType.CLIENT_BUSY, msg.key, Helper.getName(self()));
        }
    }

    /**
     * ClientRead message handler.
     * @param msg ClientRead message
     * @see ClientRead
     */
    public void onClientRead(ClientRead msg) {
        if (!this.isBusy) {
            this.isBusy = true;
            String requestId = self().path() + "/" + this.Id.toString();
            this.Id++;
            AskReadData data = new AskReadData(msg.key, requestId);
            msg.coordinator.tell(data, self());

            // logging
            Logs.client_read(msg.key, Helper.getName(self()), msg.coordinator.path().name());
        } else {
            Logs.error(ErrorType.CLIENT_BUSY, msg.key, Helper.getName(self()));
        }
    }

    /**
     * SendRead2Client message handler.
     * @param msg SendRead2Client message
     * @see SendRead2Client
     */
    public void onSendRead2Client(SendRead2Client msg) {
        this.isBusy = false;

        // logging
        Logs.read_reply_on_client(msg.value, msg.requestId, Helper.getName(getSender()), Helper.getName(self()));
    }

    /**
     * ClientUpdate message handler.
     * @param msg ClientUpdate message
     * @see ClientUpdate
     */
    public void onClientUpdate(ClientUpdate msg) {
        if (!this.isBusy) {
            this.isBusy = true;
            String requestId = self().path() + "/" + this.Id.toString();
            this.Id++;
            AskUpdateData data = new AskUpdateData(msg.key, msg.value, requestId);
            msg.coordinator.tell(data, self());

            // logging
            Logs.client_update(msg.key, msg.value, Helper.getName(self()), msg.coordinator.path().name());
        } else {
            Logs.error(ErrorType.CLIENT_BUSY, msg.key, Helper.getName(self()));
        }
    }

    /**
     * ReturnUpdate message handler.
     * Return the updated value to the client.
     * @param msg ReturnUpdate message
     * @see ReturnUpdate
     */
    public void onReturnUpdate(ReturnUpdate msg) {
        this.isBusy = false;

        // logging
        Logs.update_reply_on_client(msg.version, msg.requestId, Helper.getName(getSender()), Helper.getName(self()));
    }

    /**
     * ReturnTimeoutOnRead message handler.
     * @param msg ReturnTimeoutOnRead message
     * @see ReturnTimeoutOnRead
     */
    public void onReturnTimeoutOnRead(ReturnTimeoutOnRead msg) {
        this.isBusy = false;

        // logging
        Logs.timeout(TimeoutType.READ, msg.requestId, Helper.getName(getSender()), Helper.getName(self()));
    }

    /**
     * ReturnTimeoutOnWrite message handler.
     * @param msg ReturnTimeoutOnWrite message
     * @see ReturnTimeoutOnWrite
     */
    public void onReturnTimeoutOnWrite(ReturnTimeoutOnWrite msg) {
        this.isBusy = false;

        // logging
        Logs.timeout(TimeoutType.WRITE, msg.requestId, Helper.getName(getSender()), Helper.getName(self()));
    }

    /* ------- DEBUG & TESTING ------- */

    /**
     * StatusRequest
     * A message that tells a coordinator to start the status capture operation.
     */
    public static class StatusRequest implements Serializable {
        public final ActorRef coordinator;

        /**
         * @param coordinator ActorRef of the coordinator
         */
        public StatusRequest(ActorRef coordinator) {
            this.coordinator = coordinator;
        }
    }

    /**
     * StatusRequest handler.
     * Send a status request to the coordinator.
     * @param msg StatusRequest message
     * @see StatusRequest
     */
    public void onStatusRequest(StatusRequest msg) {
        AskStatus req = new AskStatus();
        msg.coordinator.tell(req, self());
    }

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
