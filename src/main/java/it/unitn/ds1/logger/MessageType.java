package it.unitn.ds1.logger;

public enum MessageType {
    INIT_GROUP,
    ASK_READ,
    ASK_WRITE,
    ASK_UPDATE,
    ASK_VERSION,
    ASK_GROUP,
    READ,
    WRITE,
    UPDATE,
    READ_REPLY,
    WRITE_REPLY,
    UPDATE_REPLY,
    VERSION_REPLY,
    GROUP_REPLY,
    CLIENT_READ,
    CLIENT_WRITE,
    CLIENT_UPDATE,
    TIMEOUT,
    STATUS,
    ;

    @Override
    public String toString() {
        switch (this) {
            case INIT_GROUP -> {
                return "INIT_GROUP";
            }
            case ASK_READ -> {
                return "ASK_READ";
            }
            case ASK_WRITE -> {
                return "ASK_WRITE";
            }
            case ASK_UPDATE -> {
                return "ASK_UPDATE";
            }
            case ASK_VERSION -> {
                return "ASK_VERSION";
            }
            case ASK_GROUP -> {
                return "ASK_GROUP";
            }
            case READ -> {
                return "READ";
            }
            case WRITE -> {
                return "WRITE";
            }
            case UPDATE -> {
                return "UPDATE";
            }
            case READ_REPLY -> {
                return "READ_REPLY";
            }
            case WRITE_REPLY -> {
                return "WRITE_REPLY";
            }
            case UPDATE_REPLY -> {
                return "UPDATE_REPLY";
            }
            case VERSION_REPLY -> {
                return "VERSION_REPLY";
            }
            case GROUP_REPLY -> {
                return "GROUP_REPLY";
            }
            case CLIENT_READ -> {
                return "CLIENT_READ";
            }
            case CLIENT_WRITE -> {
                return "CLIENT_WRITE";
            }
            case CLIENT_UPDATE -> {
                return "CLIENT_UPDATE";
            }
            case TIMEOUT -> {
                return "TIMEOUT";
            }
            case STATUS -> {
                return "STATUS";
            }
            default -> {
                return "";
            }
        }
    }
}
