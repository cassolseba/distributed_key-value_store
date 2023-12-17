package it.unitn.ds1.logger;

public enum ErrorType {
    LOCKED_KEY,
    UNKNOWN_KEY,
    EXISTING_KEY,
    CLIENT_BUSY,
    ;

    @Override
    public String toString() {
        switch (this) {
            case LOCKED_KEY -> {
                return "LOCKED_KEY";
            }
            case UNKNOWN_KEY -> {
                return "UNKNOWN_KEY";
            }
            case EXISTING_KEY -> {
                return "EXISTING_KEY";
            }
            case CLIENT_BUSY -> {
                return "CLIENT_BUSY";
            }
            default -> {
                return "";
            }
        }
    }
}