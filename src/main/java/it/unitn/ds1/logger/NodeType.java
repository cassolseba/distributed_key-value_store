package it.unitn.ds1.logger;

public enum NodeType {
    DATA_NODE,
    COORDINATOR,
    CLIENT,
    ;

    @Override
    public String toString() {
        switch (this) {
            case DATA_NODE -> {
                return "DATA_NODE";
            }
            case COORDINATOR -> {
                return "COORDINATOR";
            }
            case CLIENT -> {
                return "CLIENT";
            }
            default -> {
                return "";
            }
        }
    }
}
