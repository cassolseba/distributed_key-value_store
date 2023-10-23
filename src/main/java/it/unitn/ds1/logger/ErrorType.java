package it.unitn.ds1.logger;

public enum ErrorType {
    UNKNOWN_KEY,
    EXISTING_KEY,
    ;

    @Override
    public String toString() {
        switch (this) {
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