import java.io.*;
import java.net.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class Controller1 {
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

    public AtomicInteger getDstore_count() {
        return Dstore_count;
    }

    private AtomicInteger Dstore_count = new AtomicInteger(0); // number of operating Dstores

    public AtomicInteger getRebalanceCompleteACK() {
        return rebalanceCompleteACK;
    }

    private AtomicInteger rebalanceCompleteACK = new AtomicInteger(0); // counter for rebalance operation competion

    public Boolean getActiveRebalance() {
        return activeRebalance;
    }

    public void setActiveRebalance(Boolean activeRebalance) {
        this.activeRebalance = activeRebalance;
    }

    private volatile Boolean activeRebalance = false; // rebalance operation is active

    public Boolean getActiveList() {
        return activeList;
    }

    public void setActiveList(Boolean activeList) {
        this.activeList = activeList;
    }

    private volatile Boolean activeList = false; // if rebalance list request is active

    public Long getRebalanceTime() {
        return rebalanceTime;
    }

    public void setRebalanceTime(Long rebalanceTime) {
        this.rebalanceTime = rebalanceTime;
    }

    private volatile Long rebalanceTime; // tracks if it is time for rebalance

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

    public Object getDstoreJoinLock() {
        return DstoreJoinLock;
    }

    private Object DstoreJoinLock = new Object();

    public List<Integer> getListACKPorts() {
        return listACKPorts;
    }

    private List<Integer> listACKPorts = Collections.synchronizedList(new ArrayList<Integer>()); // index of active file stores

    public List<String> getFiles_activeStore() {
        return files_activeStore;
    }

    private List<String> files_activeStore = Collections.synchronizedList(new ArrayList<String>()); // index of active file stores

    public List<String> getFiles_activeRemove() {
        return files_activeRemove;
    }

    private List<String> files_activeRemove = Collections.synchronizedList(new ArrayList<String>()); // index of active file removes

    public ConcurrentHashMap<Integer, ArrayList<String>> getDstore_port_files() {
        return dstore_port_files;
    }

    private ConcurrentHashMap<Integer, ArrayList<String>> dstore_port_files = new ConcurrentHashMap<>(); // dstore port with fileslist

    public ConcurrentHashMap<Integer, Integer> getDstore_port_numbfiles() {
        return dstore_port_numbfiles;
    }

    private ConcurrentHashMap<Integer, Integer> dstore_port_numbfiles = new ConcurrentHashMap<>(); // dstore port with its file count(reduces overhead)

    public ConcurrentHashMap<String, ArrayList<Integer>> getDstore_file_ports() {
        return dstore_file_ports;
    }

    private ConcurrentHashMap<String, ArrayList<Integer>> dstore_file_ports = new ConcurrentHashMap<>(); // files with ports location relation

    public ConcurrentHashMap<String, Integer> getFile_filesize() {
        return file_filesize;
    }

    private ConcurrentHashMap<String, Integer> file_filesize = new ConcurrentHashMap<>(); // keeps track of all valid files with their sizes

    public ConcurrentHashMap<Integer, Socket> getDstore_port_Socket() {
        return dstore_port_Socket;
    }

    private ConcurrentHashMap<Integer, Socket> dstore_port_Socket = new ConcurrentHashMap<>();// dstore port - Socket relation

    public ConcurrentHashMap<String, ArrayList<Integer>> getFileToStore_ACKPorts() {
        return fileToStore_ACKPorts;
    }

    private ConcurrentHashMap<String, ArrayList<Integer>> fileToStore_ACKPorts = new ConcurrentHashMap<>(); //counts stored file confirmatios

    public ConcurrentHashMap<String, ArrayList<Integer>> getFileToRemove_ACKPorts() {
        return fileToRemove_ACKPorts;
    }

    private ConcurrentHashMap<String, ArrayList<Integer>> fileToRemove_ACKPorts = new ConcurrentHashMap<>(); //counts removed file confirmatios

    public ConcurrentHashMap<String, Integer> getFiles_addCount() {
        return files_addCount;
    }

    private ConcurrentHashMap<String, Integer> files_addCount = new ConcurrentHashMap<>(); //counts files to add from failed dstores

    public ConcurrentHashMap<String, Integer> getFiles_RemoveCount() {
        return files_RemoveCount;
    }

    private ConcurrentHashMap<String, Integer> files_RemoveCount = new ConcurrentHashMap<>(); //counts files to remove from failed dstores
    private ControllerObject1 controllerObject1;

    public Controller1(int cport, int R, int timeout, int rebalance_period) {
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
        Controller1 controller = new Controller1(cport, R, timeout, rebalance_period);
    }

    private String[] getPortsToStore(int R) { // finds R ports with least files
        Integer ports[] = new Integer[R];

        for (Integer port : dstore_port_numbfiles.keySet()) {
            int max = 0;

            for (int i = 0; i < R; i++) {
                if (ports[i] == null) {
                    max = i;
                    ports[i] = port;
                    break;
                }
                if (ports[i] != null && dstore_port_numbfiles.get(ports[i]) > dstore_port_numbfiles.get(ports[max])) {
                    max = i;
                }
            }
            if (dstore_port_numbfiles.get(port) < dstore_port_numbfiles.get(ports[max])) {
                ports[max] = port;
            }
        }

        String returnPorts[] = new String[R];
        for (int i = 0; i < R; i++) {
            returnPorts[i] = ports[i].toString();
        }
        return returnPorts;
    }

    Socket simbo;
    public synchronized void rebalanceOperation() {
        try {
            if (Dstore_count.get() < R) {
                System.out.println("*********************Not Enough Dstore for REBALANCE*********************");
                return;
            }

            synchronized (lock) {
                activeRebalance = true;
                System.out.println("*********************Rebalance waiting for store/remove*********************");
                while (files_activeRemove.size() != 0 && files_activeStore.size() != 0) {
                    continue;
                }
            }
            files_RemoveCount.clear();
            System.out.println("*********************Rebalance started*********************");
            Integer newDSCount = dstore_port_Socket.size(); // before rebalance count
            ArrayList<Integer> failedPorts = new ArrayList<>();

            //DO HERE
            System.out.println("*********************Sending LIST to all Dstores************************");
            activeList = true;
            for (Integer port : dstore_port_Socket.keySet()) { // send LIST command to each dstore
                try {
                    PrintWriter outDSP = new PrintWriter(dstore_port_Socket.get(port).getOutputStream(), true);
                    outDSP.println("LIST");
//                    ControllerLogger.getInstance().messageSent(dstore_port_Socket.get(port), Protocol.LIST_TOKEN);
                } catch (Exception e) {
                    System.err.println("Disconnected DSTORE " + port + "  ERROR:" + e);
                    newDSCount--; //if dstore disconnected lower list asks
                    failedPorts.add(port);
                }
            }

            listACKPorts.clear();
            long timeout_time = System.currentTimeMillis() + timeout;
            while (System.currentTimeMillis() <= timeout_time) {// checks if file to store has completed acknowledgements
                if (listACKPorts.size() >= newDSCount) {
                    System.out.println("*********************Confirmed LIST from all*********************");
                    break;
                }
            }
            activeList = false;

            for (Integer port : failedPorts) { // cleanup broken disconnected DStores
                //dstore_port_Socket.get(port).close();
                clearPort(port);
            }

            for (String file : dstore_file_ports.keySet()) { // clear extra files from failed removes
                if (dstore_file_ports.get(file).size() > R) {
                    int remove = dstore_file_ports.get(file).size() - R;
                    files_RemoveCount.put(file, remove);
                }
            }

            System.out.println("*********************End of LIST section*********************");
            // SECTION FOR SENDING REBALANCE OPERATION TO DSTORES WITH FILES TO SPREAD AND REMOVE
            sendRebalance();
            System.out.println("*********************Send REBALANCE commands to all*********************");
            Integer rebalanceExpected = dstore_port_Socket.size();
            timeout_time = System.currentTimeMillis() + timeout;
            while (System.currentTimeMillis() <= timeout_time) {
                if (rebalanceCompleteACK.get() >= rebalanceExpected) { // checks if file to store has completed acknowledgements
                    System.out.println("*********************REBALANCE SUCCESSFULL*********************");
                    break;
                }
            }
            System.out.println("*********************Rebalance END*********************");
            rebalanceCompleteACK.set(0);
            activeRebalance = false;
        } catch (Exception e) {
            activeRebalance = false;
            rebalanceCompleteACK.set(0);
            System.err.println("Rebalance Fatal Error: " + e);
        }
    }

    private void sendRebalance() {
        ConcurrentHashMap<Integer, ArrayList<String>> dstore_port_filesTMP = new ConcurrentHashMap<Integer, ArrayList<String>>();
        for (Integer port : dstore_port_Socket.keySet()) { // function for sorting the REBALANCE files_to_send files_to_remove
            String files_to_send = "";
            String files_to_remove = "";
            Integer files_to_send_count = 0;
            Integer files_to_remove_count = 0;
            ArrayList<String> dstore_filesTMP = new ArrayList<String>();
            for (String file : dstore_port_files.get(port)) {
                if (files_addCount.containsKey(file)) {
                    Integer[] portsToSendFile = getPortsToStoreFile(files_addCount.get(file), file);
                    for (Integer pAdd : portsToSendFile) { // add files to structure
                        dstore_filesTMP.add(file);
                        dstore_port_numbfiles.put(pAdd, dstore_port_numbfiles.get(pAdd) + 1);
                        dstore_file_ports.get(file).add(pAdd);
                    }

                    String[] portsToSendFileStr = new String[portsToSendFile.length];
                    for (int i = 0; i < portsToSendFile.length; i++) {
                        portsToSendFileStr[i] = portsToSendFile[i].toString();
                    }
                    String portcount = Integer.toString(portsToSendFile.length);
                    String portsToSendStr = String.join(" ", portsToSendFileStr);

                    files_addCount.remove(file);
                    files_to_send_count++;
                    files_to_send = files_to_send + " " + file + " " + portcount + " " + portsToSendStr;
                }

                if (!file_filesize.containsKey(file)) {
                    files_to_remove = files_to_remove + " " + file;
                    files_to_remove_count++;
                    if (dstore_port_numbfiles.containsKey(port)) {
                        dstore_port_numbfiles.put(port, dstore_port_numbfiles.get(port) - 1);
                    }
                    if (files_RemoveCount.containsKey(file)) {
                        files_RemoveCount.remove(file);
                    }
                    if (dstore_file_ports.containsKey(file)) {
                        dstore_file_ports.remove(file);
                    }
                } else {
                    if (files_RemoveCount.containsKey(file) && files_RemoveCount.get(file) > 0) {
                        files_to_remove = files_to_remove + " " + file;
                        files_to_remove_count++;
                        files_RemoveCount.put(file, files_RemoveCount.get(file) - 1);
                        if (dstore_port_numbfiles.containsKey(port)) {
                            dstore_port_numbfiles.put(port, dstore_port_numbfiles.get(port) - 1);
                        }
                        if (dstore_file_ports.containsKey(file)) {
                            if (dstore_file_ports.get(file).contains(port)) {
                                dstore_file_ports.get(file).remove(port);
                            }
                        }
                    }
                }
            }

            if (!dstore_port_filesTMP.containsKey(port)) {
                dstore_port_filesTMP.put(port, new ArrayList<>());
            }
            dstore_port_filesTMP.get(port).addAll(dstore_filesTMP);
            String message = "";
            message = " " + files_to_send_count + files_to_send + " " + files_to_remove_count + files_to_remove;
            try {
                Socket dsRebalance = dstore_port_Socket.get(port);
                PrintWriter outDSREBALANCE = new PrintWriter(dsRebalance.getOutputStream(), true);
                outDSREBALANCE.println("REBALANCE" + message);
//                ControllerLogger.getInstance().messageSent(dsRebalance, Protocol.REBALANCE_TOKEN + message);
            } catch (IOException e) {
                System.err.println("Fatal Error while sending Rebalance - " + e);
            }
        }
        for (Integer port : dstore_port_filesTMP.keySet()) {
            dstore_port_files.get(port).addAll(dstore_port_filesTMP.get(port));
        }
    }

    private synchronized void clearPort(Integer port) {
        System.out.println("CLEARING DISCONNECTED PORT " + port);
        for (String file : dstore_port_files.get(port)) {
            if (files_addCount.get(file) == null) {
                files_addCount.put(file, 1);
            } else {
                files_addCount.put(file, files_addCount.get(file) + 1);
            }
        }
        dstore_port_files.remove(port);
        dstore_port_numbfiles.remove(port);
        dstore_port_Socket.remove(port);
        ConcurrentHashMap<String, ArrayList<Integer>> tempFilePorts = new ConcurrentHashMap<String, ArrayList<Integer>>(
                dstore_file_ports);
        for (String file : tempFilePorts.keySet()) {
            if (!file_filesize.keySet().contains(file)) {
                dstore_file_ports.remove(file);
            } else if (dstore_file_ports.get(file).contains(port)) {
                dstore_file_ports.get(file).remove(port);
            }
        }
        System.out.println("CLEARED PORT " + port);
    }

    private synchronized Integer[] getPortsToStoreFile(int n, String file) { // finds R ports with least files
        Integer ports[] = new Integer[n];

        for (Integer port : dstore_port_numbfiles.keySet()) {
            int max = 0;
            for (int i = 0; i < n; i++) {
                if (ports[i] == null) {
                    max = i;
                    ports[i] = port;
                    break;
                }
                if (ports[i] != null && dstore_port_numbfiles.get(ports[i]) > dstore_port_numbfiles.get(ports[max])
                        && !dstore_port_files.get(ports[i]).contains(file)) {
                    max = i;
                }
            }
            if (dstore_port_numbfiles.get(port) <= dstore_port_numbfiles.get(ports[max])
                    && !dstore_port_files.get(port).contains(file)) {
                ports[max] = port;
            } else if (dstore_port_numbfiles.get(port) > dstore_port_numbfiles.get(ports[max])
                    && dstore_port_files.get(ports[max]).contains(file)) {
                ports[max] = port;
            }
        }

        return ports;
    }

}