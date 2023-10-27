package it.unitn.ds1.logger;

public enum ErrorType {
    LOCKED_KEY,
    UNKNOWN_KEY,
    EXISTING_KEY,
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
            default -> {
                return "";
            }
        }
    }
}