package it.unitn.ds1.logger;

import it.unitn.ds1.managers.DataManager;

import java.util.Map;

public class Logs {
    private final static long START_TIME = System.currentTimeMillis();
    public final static String FROM_NODE = " | from %s: %s";
    public final static String TO_NODE = " | to %s: %s";
    public final static String IN_NODE = " | in %s: %s";
    private final static String LOG = "%s: %s | %s |";
    private final static String HEADER = "MESSAGE : TIME | CONTENT | FROM | TO";
    private final static String WRITE_FORMAT = "key: %d, value: %s";
    private final static String REQUEST_FORMAT = "key: %d, request id: %s";
    private final static String UPDATE_FORMAT = "key: %d, new value: %s, request id: %s";
    private final static String DATA_FORMAT = "value: %s, version: %s, request id: %s";
    private final static String RESULT_FORMAT = "value: %s, request id: %s";
    private final static String VERSION_FORMAT = "version: %d, request id: %s";
    private final static String ITEMS_FORMAT = "keys: %s";
    private final static String KEY_FORMAT = "key: %d";
    private final static String NODE_FORMAT = "node: %s";
    private final static String TIMEOUT_FORMAT = "request id: %s";
    private final static String STATUS = "key: %d, value: %s, version: %d";
    private final static String TEST = "Running test %d: %s\n";

    /**
     * Print a simple header for logs.
     */
    public static void printHeader() { System.out.println(HEADER); }

    /**
     *
     */
    public static void printStartupInfo(int N, int W, int R, int dataNodeCount, int clientCount) {
        String init = String.format("Initializing: database with %d datanode and %d client\n",
                dataNodeCount,
                clientCount);
        String info = String.format("Replicas: %d, write quorum: %d, read quorum: %d\n", N, W, R);
        System.out.println(init + info);
    }

    /**
     *
     */
    public static void printClientInit() { System.out.println("\nGetting client references...\n"); }

    /**
     *
     */
    public static void printDatanodeInit() { System.out.println("Getting data nodes references...\n"); }

    /**
     *
     */
    public static void printDataInit() { System.out.println("Writing data into data nodes...\n"); }

    /**
     *
     */
    public static void printStartStatusCheck() { System.out.println("\nLaunching status check...\n"); }

    /**
     *
     */
    public static void printEndStatusCheck() { System.out.println("\nEnd status check...\n"); }

    /**
     *
     */
    public static void printRunTest(int n, String test) {
        String log = String.format(TEST, n, test);
        System.out.println(log);
    }

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
        String msg = String.format(REQUEST_FORMAT, key, "") +
                String.format(FROM_NODE, NodeType.CLIENT, client) +
                String.format(TO_NODE, NodeType.COORDINATOR, coordinator);
        printLog(MessageType.CLIENT_READ, msg);
    }

    public static void client_update(int key, String new_value, String client, String coordinator) {
        String msg = String.format(UPDATE_FORMAT, key, new_value, "") +
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
        String msg = String.format(REQUEST_FORMAT, key, request_id) +
                String.format(FROM_NODE, NodeType.CLIENT, client) +
                String.format(TO_NODE, NodeType.COORDINATOR, coordinator);
        printLog(MessageType.ASK_READ, msg);
    }

    public static void read(int key, String request_id, String coordinator, String node) {
        String msg = String.format(REQUEST_FORMAT, key, request_id) +
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
        printLog(MessageType.READ_RESULT, msg);
    }

    public static void ask_update(int key, String value, String request_id, String client, String coordinator) {
        String msg = String.format(UPDATE_FORMAT, key, value, request_id) +
                String.format(FROM_NODE, NodeType.CLIENT, client) +
                String.format(TO_NODE, NodeType.COORDINATOR, coordinator);
        printLog(MessageType.ASK_UPDATE, msg);
    }

    public static void update(int key, String value, String coordinator, String node) {
        String msg = String.format(UPDATE_FORMAT, key, value, "") +
                String.format(FROM_NODE, NodeType.COORDINATOR, coordinator) +
                String.format(TO_NODE, NodeType.DATA_NODE, node);
        printLog(MessageType.UPDATE, msg);
    }

    public static void update_reply_on_client(int version, String request_id, String node, String client) {
        String msg = String.format(VERSION_FORMAT, version, request_id) +
                String.format(FROM_NODE, NodeType.DATA_NODE, node) +
                String.format(TO_NODE, NodeType.CLIENT, client);
        printLog(MessageType.UPDATE_REPLY, msg);
    }
    public static void ask_version(int key, String request_id, String coordinator, String node) {
        String msg = String.format(REQUEST_FORMAT, key, request_id) +
                String.format(FROM_NODE, NodeType.COORDINATOR, coordinator) +
                String.format(TO_NODE, NodeType.DATA_NODE, node);
        printLog(MessageType.ASK_VERSION, msg);
    }

    public static void version_reply(int version, String request_id, String node, String coordinator) {
        String msg = String.format(VERSION_FORMAT, version, request_id) +
                String.format(FROM_NODE, NodeType.DATA_NODE, node) +
                String.format(FROM_NODE, NodeType.COORDINATOR, coordinator);
        printLog(MessageType.VERSION_REPLY, msg);
    }

    public static void ask_group(String joining, String node) {
        String msg = String.format(FROM_NODE, NodeType.DATA_NODE, joining) +
                String.format(TO_NODE, NodeType.DATA_NODE, node);
        printLog(MessageType.ASK_GROUP, msg);
    }

    public static void group_reply(String node, String joining) {
        String msg = String.format(FROM_NODE, NodeType.DATA_NODE, node) +
                String.format(TO_NODE, NodeType.DATA_NODE, joining);
        printLog(MessageType.GROUP_REPLY, msg);
    }

    public static void ask_keys(String sender, String receiver) {
        String msg = String.format(FROM_NODE, NodeType.DATA_NODE, sender) +
                String.format(TO_NODE, NodeType.DATA_NODE, receiver);
        printLog(MessageType.ASK_KEYS, msg);
    }

    public static void items_reply(String keys, String sender, String receiver) {
        String msg = String.format(ITEMS_FORMAT, keys) +
                String.format(FROM_NODE, NodeType.DATA_NODE, sender) +
                String.format(TO_NODE, NodeType.DATA_NODE, receiver);
        printLog(MessageType.ITEMS_REPLY, msg);
    }

    public static void ask_data(int key, String sender, String receiver) {
        String msg = String.format(KEY_FORMAT, key) +
                String.format(FROM_NODE, NodeType.DATA_NODE, sender) +
                String.format(TO_NODE, NodeType.DATA_NODE, receiver);
        printLog(MessageType.ASK_DATA, msg);
    }

    public static void data_reply(int key, DataManager.Data data, String sender, String receiver) {
        String msg = String.format(KEY_FORMAT, key) + " " +
                String.format(DATA_FORMAT, data.getValue(), data.getVersion(), "") +
                String.format(FROM_NODE, NodeType.DATA_NODE, sender) +
                String.format(TO_NODE, NodeType.DATA_NODE, receiver);
        printLog(MessageType.DATA_REPLY, msg);
    }

    public static void join(int key, String sender, String receiver) {
        String msg = String.format(KEY_FORMAT, key) +
                String.format(FROM_NODE, NodeType.DATA_NODE, sender) +
                String.format(TO_NODE, NodeType.DATA_NODE, receiver);
        printLog(MessageType.JOIN, msg);
    }

    public static void ask_leave(String sender, String receiver) {
        String msg = String.format(FROM_NODE, NodeType.DATA_NODE, sender) +
                String.format(TO_NODE, NodeType.DATA_NODE, receiver);
        printLog(MessageType.ASK_LEAVE, msg);
    }

    public static void leave(String sender, String receiver) {
        String msg = String.format(FROM_NODE, NodeType.DATA_NODE, sender) +
                String.format(TO_NODE, NodeType.DATA_NODE, receiver);
        printLog(MessageType.LEAVE, msg);
    }

    public static void crash(String sender, String receiver) {
        String msg = String.format(FROM_NODE, NodeType.DATA_NODE, sender) +
                String.format(TO_NODE, NodeType.DATA_NODE, receiver);
        printLog(MessageType.CRASH, msg);
    }

    public static void ask_recover(String node, String sender, String receiver) {
        String msg = String.format(NODE_FORMAT, node) +
                String.format(FROM_NODE, NodeType.DATA_NODE, sender) +
                String.format(TO_NODE, NodeType.DATA_NODE, receiver);
        printLog(MessageType.RECOVER, msg);
    }

    public static void data_recover(Map<Integer, DataManager.Data> data, String sender, String receiver) {
        String spaces = " ";
        spaces = spaces.repeat(24);

        StringBuilder msg = new StringBuilder();
        for (Map.Entry<Integer, DataManager.Data> entry: data.entrySet()) {
            msg.append(String.format(KEY_FORMAT, entry.getKey()));
            msg.append(" ");
            msg.append(String.format(DATA_FORMAT, entry.getValue().getValue(), entry.getValue().getVersion(), "none"));
            msg.append("\n");
            msg.append(spaces);
        }
        msg.append(String.format(FROM_NODE, NodeType.DATA_NODE, sender));
        msg.append(String.format(TO_NODE, NodeType.DATA_NODE, receiver));
        printLog(MessageType.DATA_REPLY, msg.toString());
    }

    public static void timeout(TimeoutType type, String request_id, String sender, String receiver) {
        switch (type) {
            case RECOVER -> {
                String msg = String.format(FROM_NODE, NodeType.DATA_NODE, sender) +
                        String.format(TO_NODE, NodeType.DATA_NODE, receiver);
                printLog(MessageType.RECOVER_TIMEOUT, msg);
            }
            case READ -> {
                String msg = String.format(TIMEOUT_FORMAT, request_id) +
                        String.format(FROM_NODE, NodeType.DATA_NODE, sender) +
                        String.format(TO_NODE, NodeType.CLIENT, receiver);
                printLog(MessageType.READ_TIMEOUT, msg);
            }
            case WRITE -> {
                String msg = String.format(TIMEOUT_FORMAT, request_id) +
                        String.format(FROM_NODE, NodeType.DATA_NODE, sender) +
                        String.format(TO_NODE, NodeType.CLIENT, receiver);
                printLog(MessageType.WRITE_TIMEOUT, msg);
            }
            default -> {}
        }
    }

    public static void error(ErrorType type, int key, String sender) {
        final String msg = String.format(KEY_FORMAT, key) +
                String.format(IN_NODE, NodeType.DATA_NODE, sender);

        switch (type) {
            case LOCKED_KEY -> printLog(MessageType.LOCKED_KEY_ERROR, msg);
            case UNKNOWN_KEY -> printLog(MessageType.UNKNOWN_KEY_ERROR, msg);
            case EXISTING_KEY -> printLog(MessageType.EXISTING_KEY_ERROR, msg);
            default -> {}
        }
    }


    /**
     * Produce the log for a STATUS CHECK operation
     * @param key is the key in the stored pair
     * @param value is the value in the stored pair
     * @param version is the version of the stored pair
     * @param node is the data node storing pair
     */
    public static void status(int key, String value, int version, String node) {
        String msg = String.format(STATUS, key, value, version) +
                String.format(FROM_NODE, NodeType.DATA_NODE, node);
        printLog(MessageType.STATUS, msg);
    }
}
