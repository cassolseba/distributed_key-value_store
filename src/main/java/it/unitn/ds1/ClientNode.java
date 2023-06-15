package it.unitn.ds1;
import akka.actor.*;

public class ClientNode extends AbstractActor {

    public ClientNode() {}

    static public Props props() {
        return Props.create(ClientNode.class, () -> new ClientNode());
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
            .build();
    }
}
