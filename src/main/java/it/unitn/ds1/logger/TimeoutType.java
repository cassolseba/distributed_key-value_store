package it.unitn.ds1.logger;

public enum TimeoutType {
    RECOVER,
    READ,
    WRITE,
    ;

    @Override
    public String toString() {
        switch (this) {
            case RECOVER -> {
                return "RECOVER";
            }
            case READ -> {
                return "READ";
            }
            case WRITE -> {
                return "WRITE";
            }
            default -> {
                return "";
            }
        }
    }
}