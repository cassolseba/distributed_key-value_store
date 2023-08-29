package it.unitn.ds1.utils;

import akka.actor.ActorRef;

public class Helper {
    public static String getName(ActorRef actorRef) {
        return actorRef.path().name();
    }
}
