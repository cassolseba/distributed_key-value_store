package it.unitn.ds1.logger;

public enum MessageType {
    INIT_GROUP,
    ASK_READ,
    ASK_WRITE,
    ASK_UPDATE,
    ASK_VERSION,
    ASK_GROUP,
    ASK_KEYS,
    ASK_DATA,
    ASK_LEAVE,
    READ,
    WRITE,
    UPDATE,
    JOIN,
    LEAVE,
    READ_REPLY,
    WRITE_REPLY,
    UPDATE_REPLY,
    VERSION_REPLY,
    GROUP_REPLY,
    ITEMS_REPLY,
    DATA_REPLY,
    READ_RESULT,
    CLIENT_READ,
    CLIENT_WRITE,
    CLIENT_UPDATE,
    CRASH,
    RECOVER,
    RECOVER_TIMEOUT,
    READ_TIMEOUT,
    WRITE_TIMEOUT,
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
            case ASK_KEYS -> {
                return "ASK_KEYS";
            }
            case ASK_DATA -> {
                return "ASK_DATA";
            }
            case ASK_LEAVE -> {
                return "ASK_LEAVE";
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
            case JOIN -> {
                return "JOIN";
            }
            case LEAVE -> {
                return "LEAVE";
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
            case ITEMS_REPLY -> {
                return "ITEMS_REPLY";
            }
            case DATA_REPLY -> {
                return "DATA_REPLY";
            }
            case READ_RESULT -> {
                return "READ_RESULT";
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
            case CRASH -> {
                return "CRASH";
            }
            case RECOVER -> {
                return "RECOVER";
            }
            case RECOVER_TIMEOUT -> {
                return "RECOVER_TIMEOUT";
            }
            case READ_TIMEOUT -> {
                return "READ_TIMEOUT";
            }
            case WRITE_TIMEOUT -> {
                return "WRITE_TIMEOUT";
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
