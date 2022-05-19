import java.io.*;
import java.net.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class Controller {
    private Integer cport; // port of Controller

    public Integer getR() {
        return R;
    }

    private Integer R; // Replication factor - number of Dstores

    public Integer getTimeout() {
        return timeout;
    }

    private Integer timeout; // Timeout period in ms
    private Integer rebalance_period; // period to call Rebalance in ms

    public AtomicInteger getDstoreCount() {
        return DstoreCount;
    }

    private AtomicInteger DstoreCount = new AtomicInteger(0); // number of operating Dstores


    public Boolean getActiveList() {
        return activeList;
    }

    public void setActiveList(Boolean activeList) {
        this.activeList = activeList;
    }

    private volatile Boolean activeList = false; // if rebalance list request is active


    public Object getLock() {
        return lock;
    }

    private Object lock = new Object();

    public Object getRemoveLock() {
        return removeLock;
    }

    private Object removeLock = new Object();

    public Object getStoreLock() {
        return storeLock;
    }

    private Object storeLock = new Object();

    public Object getDstorejoinlock() {
        return Dstorejoinlock;
    }

    private Object Dstorejoinlock = new Object();

    public List<Integer> getAckPortsList() {
        return ackPortsList;
    }

    private List<Integer> ackPortsList = Collections.synchronizedList(new ArrayList<Integer>()); // index of active file stores

    public List<String> getActiveFileStore() {
        return activeFileStore;
    }

    private List<String> activeFileStore = Collections.synchronizedList(new ArrayList<String>()); // index of active file stores

    public List<String> getActiveFileRemove() {
        return activeFileRemove;
    }

    private List<String> activeFileRemove = Collections.synchronizedList(new ArrayList<String>()); // index of active file removes

    public ConcurrentHashMap<Integer, ArrayList<String>> getDstoreFileList() {
        return dstoreFileList;
    }

    private ConcurrentHashMap<Integer, ArrayList<String>> dstoreFileList = new ConcurrentHashMap<>(); // dstore port with fileslist

    public ConcurrentHashMap<Integer, Integer> getDstoreFileCount() {
        return dstoreFileCount;
    }

    private ConcurrentHashMap<Integer, Integer> dstoreFileCount = new ConcurrentHashMap<>(); // dstore port with its file count(reduces overhead)

    public ConcurrentHashMap<String, ArrayList<Integer>> getFilePortList() {
        return filePortList;
    }

    private ConcurrentHashMap<String, ArrayList<Integer>> filePortList = new ConcurrentHashMap<>(); // files with ports location relation

    public ConcurrentHashMap<String, Integer> getFile_filesize() {
        return fileFileSize;
    }

    private ConcurrentHashMap<String, Integer> fileFileSize = new ConcurrentHashMap<>(); // keeps track of all valid files with their sizes

    public ConcurrentHashMap<Integer, Socket> getDstore_port_Socket() {
        return dstorePortSocket;
    }

    private ConcurrentHashMap<Integer, Socket> dstorePortSocket = new ConcurrentHashMap<>();// dstore port - Socket relation

    public ConcurrentHashMap<String, ArrayList<Integer>> getAckPortsFileStore() {
        return ackPortsFileStore;
    }

    private ConcurrentHashMap<String, ArrayList<Integer>> ackPortsFileStore = new ConcurrentHashMap<>(); //counts stored file confirmatios

    public ConcurrentHashMap<String, ArrayList<Integer>> getAckPortsFileREmove() {
        return ackPortsFileREmove;
    }

    private ConcurrentHashMap<String, ArrayList<Integer>> ackPortsFileREmove = new ConcurrentHashMap<>(); //counts removed file confirmatios

    public ConcurrentHashMap<String, Integer> getFiles_addCount() {
        return files_addCount;
    }

    private ConcurrentHashMap<String, Integer> files_addCount = new ConcurrentHashMap<>(); //counts files to add from failed dstores

    public ConcurrentHashMap<String, Integer> getRemoveCountFiles() {
        return removeCountFiles;
    }

    private ConcurrentHashMap<String, Integer> removeCountFiles = new ConcurrentHashMap<>(); //counts files to remove from failed dstores
    private ControllerObject1 controllerObject1;

    public Controller(int cport, int R, int timeout, int rebalance_period) {
        this.cport = cport;
        this.R = R;
        this.timeout = timeout;
        this.rebalance_period = rebalance_period;
        try {
//            ControllerLogger.init(Logger.LoggingType.ON_FILE_AND_TERMINAL);
        } catch (Exception e) {
            e.printStackTrace();
        }
        this.openController();
    }

    public void openController () {
        try {
            ServerSocket ss = new ServerSocket(cport);
            /*new Thread(() -> { // REBALANCE OPERATION THREAD
                while (true) {
                    this.rebalanceTime = System.currentTimeMillis() + rebalance_period;
                    while (System.currentTimeMillis() <= this.rebalanceTime) {
                        continue;
                    }
                    rebalanceOperation();
                    activeRebalance = false;
                }
            }).start();*/

            for (;;) {
                System.out.println("Open for Connection on port " + cport);
                Socket client = ss.accept();

                controllerObject1 = new ControllerObject1(this);
                Handler handler = new Handler(client,controllerObject1);
                new Thread(handler).start();
            }
        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("openController could not be started, ServerSocket failed");
        }
    }

    public static void main(String[] args) throws IOException {
        int cport = Integer.parseInt(args[0]);
        int R = Integer.parseInt(args[1]);
        int timeout = Integer.parseInt(args[2]);
        int rebalance_period = Integer.parseInt(args[3]);
        Controller controller = new Controller(cport, R, timeout, rebalance_period);
    }

    private String[] getPortsToStore(int R) { // finds R ports with least files
        Integer ports[] = new Integer[R];

        for (Integer port : dstoreFileCount.keySet()) {
            int max = 0;

            for (int i = 0; i < R; i++) {
                if (ports[i] == null) {
                    max = i;
                    ports[i] = port;
                    break;
                }
                if (ports[i] != null && dstoreFileCount.get(ports[i]) > dstoreFileCount.get(ports[max])) {
                    max = i;
                }
            }
            if (dstoreFileCount.get(port) < dstoreFileCount.get(ports[max])) {
                ports[max] = port;
            }
        }

        String returnPorts[] = new String[R];
        for (int i = 0; i < R; i++) {
            returnPorts[i] = ports[i].toString();
        }
        return returnPorts;
    }
}