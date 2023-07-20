package it.unitn.ds1.logger;

public class Logs {
    private final static long START_TIME = System.currentTimeMillis();
    public final static String FROM_NODE = " from %s: %s";
    public final static String TO_NODE = " to %s: %s";
    private final static String LOG = "%s: %s - %s";
    private final static String WRITE_FORMAT = "key: %d, value: %s";
    private final static String READ_FORMAT = "key: %d, request id: %s";
    private final static String UPDATE_FORMAT = "key: %d, new value: %s";
    private final static String DATA_FORMAT = "value: %s, version: %s, request id: %s";
    private final static String RESULT_FORMAT = "value: %s, request id: %s";


    /**
     * Compose the entire log message.
     * @param type is an enum that specify the type of message
     * @param msg the message produced by its relative function that previously formats the string
     */
    public static void printLog(MessageType type, String msg) {
        long time = System.currentTimeMillis() - START_TIME;
        long minutes = time / 60000;
        long seconds = time / 1000;
        long millis = time % 1000;
        String time_msg = String.format("%02d:%02d,%03d", minutes, seconds, millis);

        String log = String.format(LOG, type, time_msg, msg);
        System.out.println(log);
    }

    /**
     * Produce the log message for the group initialization that happens in every data node
     * @param node is the node in where the group init happens
     */
    public static void init_group(String node) {
        String msg = String.format(FROM_NODE, NodeType.DATA_NODE, node);
        printLog(MessageType.INIT_GROUP, msg);
    }

    /**
     * Produce the log message for a CLIENT_WRITE operation
     * @param key is the key to write
     * @param value is the value to write
     * @param client is the node client that has to send the CLIENT_WRITE request
     * @param coordinator is the receiver of CLIENT_WRITE request
     */
    public static void client_write(int key, String value, String client, String coordinator) {
        String msg = String.format(WRITE_FORMAT, key, value) +
                String.format(FROM_NODE, NodeType.CLIENT, client) +
                String.format(TO_NODE, NodeType.COORDINATOR, coordinator);
        printLog(MessageType.CLIENT_WRITE, msg);
    }

    public static void client_read(int key, String client, String coordinator) {
        String msg = String.format(READ_FORMAT, key, "") +
                String.format(FROM_NODE, NodeType.CLIENT, client) +
                String.format(TO_NODE, NodeType.COORDINATOR, coordinator);
        printLog(MessageType.CLIENT_READ, msg);
    }

    public static void client_update(int key, String new_value, String client, String coordinator) {
        String msg = String.format(UPDATE_FORMAT, key, new_value) +
                String.format(FROM_NODE, NodeType.CLIENT, client) +
                String.format(TO_NODE, NodeType.COORDINATOR, coordinator);
        printLog(MessageType.CLIENT_UPDATE, msg);
    }

    public static void ask_write(int key, String value, String client, String coordinator) {
        String msg = String.format(WRITE_FORMAT, key, value) +
                String.format(FROM_NODE, NodeType.CLIENT, client) +
                String.format(TO_NODE, NodeType.COORDINATOR, coordinator);
        printLog(MessageType.ASK_WRITE, msg);
    }

    public static void write(int key, String value, String coordinator, String node) {
        String msg = String.format(WRITE_FORMAT, key, value) +
                String.format(FROM_NODE, NodeType.COORDINATOR, coordinator) +
                String.format(TO_NODE, NodeType.DATA_NODE, node);
        printLog(MessageType.WRITE, msg);
    }

    public static void ask_read(int key, String request_id, String client, String coordinator) {
        String msg = String.format(READ_FORMAT, key, request_id) +
                String.format(FROM_NODE, NodeType.CLIENT, client) +
                String.format(TO_NODE, NodeType.COORDINATOR, coordinator);
        printLog(MessageType.ASK_READ, msg);
    }

    public static void read(int key, String request_id, String coordinator, String node) {
        String msg = String.format(READ_FORMAT, key, request_id) +
                String.format(FROM_NODE, NodeType.COORDINATOR, coordinator) +
                String.format(TO_NODE, NodeType.DATA_NODE, node);
        printLog(MessageType.READ, msg);
    }

    public static void read_reply(String value, int version, String request_id, String node, String client) {
        String msg = String.format(DATA_FORMAT, value, version, request_id) +
                String.format(FROM_NODE, NodeType.DATA_NODE, node) +
                String.format(TO_NODE, NodeType.CLIENT, client);
        printLog(MessageType.READ_REPLY, msg);
    }

    public static void read_reply_on_client(String value, String request_id, String node, String client) {
        String msg = String.format(RESULT_FORMAT, value, request_id) +
                String.format(FROM_NODE, NodeType.DATA_NODE, node) +
                String.format(TO_NODE, NodeType.CLIENT, client);
        printLog(MessageType.READ_REPLY, msg);
    }
    public static void timeout() {}


    // READ OPERATIONS

    // WRITE OPERATIONS

}
