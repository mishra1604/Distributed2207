import java.io.*;
import java.net.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class Controller {
    private Integer cport; // port of Controller
    private Integer R; // Replication factor - number of Dstores
    private Integer timeout; // Timeout period in ms
    private Integer rebalance_period; // period to call Rebalance in ms
    private AtomicInteger Dstore_count = new AtomicInteger(0); // number of operating Dstores
    private AtomicInteger rebalanceCompleteACK = new AtomicInteger(0); // counter for rebalance operation competion
    private volatile Boolean activeRebalance = false; // rebalance operation is active
    private volatile Boolean activeList = false; // if rebalance list request is active
    private volatile Long rebalanceTime; // tracks if it is time for rebalance
    private Object lock = new Object();
    private Object removeLock = new Object();
    private Object storeLock = new Object();
    private Object DstoreJoinLock = new Object();
    private List<Integer> listACKPorts = Collections.synchronizedList(new ArrayList<Integer>()); // index of active file stores
    private List<String> files_activeStore = Collections.synchronizedList(new ArrayList<String>()); // index of active file stores
    private List<String> files_activeRemove = Collections.synchronizedList(new ArrayList<String>()); // index of active file removes
    private ConcurrentHashMap<Integer, ArrayList<String>> dstore_port_files = new ConcurrentHashMap<>(); // dstore port with fileslist
    private ConcurrentHashMap<Integer, Integer> dstore_port_numbfiles = new ConcurrentHashMap<>(); // dstore port with its file count(reduces overhead)
    private ConcurrentHashMap<String, ArrayList<Integer>> dstore_file_ports = new ConcurrentHashMap<>(); // files with ports location relation
    private ConcurrentHashMap<String, Integer> file_filesize = new ConcurrentHashMap<>(); // keeps track of all valid files with their sizes
    private ConcurrentHashMap<Integer, Socket> dstore_port_Socket = new ConcurrentHashMap<>();// dstore port - Socket relation
    private ConcurrentHashMap<String, ArrayList<Integer>> fileToStore_ACKPorts = new ConcurrentHashMap<>(); //counts stored file confirmatios
    private ConcurrentHashMap<String, ArrayList<Integer>> fileToRemove_ACKPorts = new ConcurrentHashMap<>(); //counts removed file confirmatios
    private ConcurrentHashMap<String, Integer> files_addCount = new ConcurrentHashMap<>(); //counts files to add from failed dstores
    private ConcurrentHashMap<String, Integer> files_RemoveCount = new ConcurrentHashMap<>(); //counts files to remove from failed dstores

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
        this.startController();
    }

    public void getList (String[] data, PrintWriter outClient) {
        if (data.length != 1) {
            System.out.println("Malformed message received for LIST by Client");
        } // log error and continue
        if (Dstore_count.get() < R) {
            outClient.println("ERROR_LOAD");
//                                                                    ControllerLogger.getInstance().messageSent(client,
//                                                                            Protocol.ERROR_LOAD_TOKEN);
        } else if (file_filesize.size() != 0) {
            String filesList = String.join(" ", file_filesize.keySet());
            outClient.println("LIST" + " " + filesList);
//                                                                    ControllerLogger.getInstance().messageSent(client,
//                                                                            Protocol.LIST_TOKEN + " " + filesList);
        } else {
            outClient.println("LIST");
//                                                                    ControllerLogger.getInstance().messageSent(client, Protocol.LIST_TOKEN);
        }
    }

    /**
     * @param dataline String, sent from the client on cport to the controller
     * Method returns command from the Client: STORE LOAD REMOVE LIST
     * */

    public String getCommand (String dataline) {
        String[] data = dataline.split(" ");
        String command;
        if (data.length == 1) {
            command = dataline.trim();
            data[0] = command;
            return command;
        }
            command = data[0];
            return command;
    }

    public void removeACK(String[] data, Integer dstoreport) {
        synchronized (removeLock) {
            if (fileToRemove_ACKPorts.containsKey(data[1])) {
                fileToRemove_ACKPorts.get(data[1]).remove(dstoreport);
            } // removing dstore with ack from list
        }
        dstore_port_files.get(dstoreport).remove(data[1]); //removes file from map of port
        dstore_file_ports.get(data[1]).remove(dstoreport);// remove port from file - ports map
        dstore_port_numbfiles.put(dstoreport,
                dstore_port_numbfiles.get(dstoreport) - 1); // suspend 1 from file count
    }

    public void dstoreListReceived (Integer dstoreport, String[] data, Socket client) {
        ArrayList<String> filelist = new ArrayList<String>(Arrays.asList(data));
        filelist.remove(0); // remove command entry
        dstore_port_numbfiles.put(dstoreport, filelist.size()); // updates port/numbfiles hashmap
        dstore_port_files.put(dstoreport, filelist); // puts list in hashmap
        dstore_port_Socket.put(dstoreport, client);
        for (String string : filelist) {
            if (dstore_file_ports.get(string) == null) {
                dstore_file_ports.put(string, new ArrayList<Integer>());
            }
            if (!dstore_file_ports.get(string).contains(dstoreport))
                dstore_file_ports.get(string).add(dstoreport); // puts the given file the port that its in
        }
        listACKPorts.add(dstoreport);
    }

    public void remove(String[] data, PrintWriter outClient) {
        if (data.length != 2) {
            System.err.println("Malformed message received for REMOVE");
        } // log error and continue
        String filename = data[1];
        if (Dstore_count.get() < R) {
            outClient.println("ERROR_NOT_ENOUGH_DSTORES");
//                                                            ControllerLogger.getInstance().messageSent(client,
//                                                                    Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
        } else if (!file_filesize.containsKey(filename)
                || files_activeStore.contains(filename)) {
            outClient.println("ERROR_FILE_DOES_NOT_EXIST");
//                                                            ControllerLogger.getInstance().messageSent(client,
//                                                                    Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
        } else {
            synchronized (lock) {
                if (files_activeRemove.contains(filename)
                        || !file_filesize.containsKey(filename)) { // INDEX CHECKS FOR CONCURENT FILE STORE
                    outClient.println("ERROR_FILE_DOES_EXIST");
//                                                                    ControllerLogger.getInstance().messageSent(client,
//                                                                            Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
                } else {
                    while (activeRebalance) { // waiting for rebalance to end
                        continue;
                    }
                    files_activeRemove.add(filename);
                    file_filesize.remove(filename);// remove file_filesize so if broken rebalance should fix
                }
            }
            fileToRemove_ACKPorts.put(filename,
                    new ArrayList<>(dstore_file_ports.get(filename))); // initializes the ports that wait for remove
            files_addCount.remove(filename);

            synchronized (removeLock) {
                for (Integer port : fileToRemove_ACKPorts.get(filename)) { // send ports file to delete
                    Socket dstoreSocket = dstore_port_Socket.get(port);
                    PrintWriter outDstore = null;
                    try {
                        outDstore = new PrintWriter(
                                dstoreSocket.getOutputStream(), true);
                    } catch (IOException e) {
                        System.out.println("Remove() error, Controller.java");
                    }
                    outDstore.println("REMOVE" + " " + filename);
//                                                                    ControllerLogger.getInstance().messageSent(dstoreSocket,
//                                                                            Protocol.REMOVE_TOKEN + " " + filename);
                }
            }

            boolean success_Remove = false;
            long timeout_time = System.currentTimeMillis() + timeout;
            while (System.currentTimeMillis() <= timeout_time) {
                if (fileToRemove_ACKPorts.get(filename).size() == 0) { // checks if file to store has completed acknowledgements
                    outClient.println("REMOVE_COMPLETE");
//                                                                    ControllerLogger.getInstance().messageSent(client,
//                                                                            Protocol.REMOVE_COMPLETE_TOKEN);
                    dstore_file_ports.remove(filename);
                    success_Remove = true;
                    break;
                }
            }

            if (!success_Remove) {
                System.err.println("REMOVE timed out for: " + filename);
            }
            fileToRemove_ACKPorts.remove(filename);
            files_activeRemove.remove(filename); // remove file ActiveRemove from INDEX
        }
    }

    public void load (String[] data, PrintWriter outClient, String dataline, ConcurrentHashMap<String, ArrayList<Integer>> dstore_file_portsLeftReload) {
        if (data.length != 2) {
            System.out.println("Malformed message received for LOAD/RELOAD");
        } // log error and continue
        String filename = data[1];
        if (Dstore_count.get() < R) {
            outClient.println("ERROR_NOT_ENOUGH_DSTORES");
//                                                        ControllerLogger.getInstance().messageSent(client,
//                                                                Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
        } else {
            if (!file_filesize.containsKey(filename)) { // CHECKS FILE CONTAINS AND INDEX
                outClient.println("ERROR_FILE_DOES_NOT_EXIST");
//                                                            ControllerLogger.getInstance().messageSent(client,
//                                                                    Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
            } else {
                if (files_activeStore.contains(filename)
                        || files_activeRemove.contains(filename)) { // INDEX CHECKS FOR CONCURENT FILE STORE
                    String dt;
                    if (getCommand(dataline).equals("LOAD")) {
                        outClient.println("ERROR_FILE_DOES_NOT_EXIST_TOKEN");
//                                                                    ControllerLogger.getInstance().messageSent(client,
//                                                                            Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
                    } else {
                        outClient.println("ERROR_LOAD");
//                                                                    ControllerLogger.getInstance().messageSent(client,
//                                                                            Protocol.ERROR_LOAD_TOKEN);
                    }
                }

                while (activeRebalance) {
                    continue;
                }

                if (getCommand(dataline).equals("LOAD")) {
                    dstore_file_portsLeftReload.put(filename,
                            new ArrayList<>(dstore_file_ports.get(filename)));
                    outClient.println("LOAD_FROM" + " "
                            + dstore_file_portsLeftReload.get(filename).get(0) + " "
                            + file_filesize.get(filename));
//                                                                ControllerLogger.getInstance().messageSent(client,
//                                                                        Protocol.LOAD_FROM_TOKEN + " "
//                                                                                + dstore_file_portsLeftReload.get(filename).get(0)
//                                                                                + " " + file_filesize.get(filename));
                    dstore_file_portsLeftReload.get(filename).remove(0);
                } else {
                    if (dstore_file_portsLeftReload.get(filename) != null
                            && !dstore_file_portsLeftReload.get(filename).isEmpty()) {
                        outClient.println("LOAD_FROM" + " "
                                + dstore_file_portsLeftReload.get(filename).get(0) + " "
                                + file_filesize.get(filename));
//                                                                    ControllerLogger.getInstance().messageSent(client,
//                                                                            Protocol.LOAD_FROM_TOKEN
//                                                                                    + " " + dstore_file_portsLeftReload
//                                                                                    .get(filename).get(0)
//                                                                                    + " " + file_filesize.get(filename));
                        dstore_file_portsLeftReload.get(filename).remove(0);
                    } else {
                        outClient.println("ERROR_LOAD");
//                                                                    ControllerLogger.getInstance().messageSent(client,
//                                                                            Protocol.ERROR_LOAD_TOKEN);
                    }
                }

            }
        }
    }

    public void store (String[] data, PrintWriter outClient ) {
        if (data.length != 3) {
            System.out.println("INCORRECT MESSAGE STRUCTURE FOR STORE");
        }
        String filename = data[1];
        Integer filesize = Integer.parseInt(data[2]);

        if (Dstore_count.get() < R) {
            outClient.println("ERROR_NOT_ENOUGH_DSTORES");
//                                            ControllerLogger.getInstance().messageSent(client,
//                                                    Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
        } else if (file_filesize.get(filename) != null
                || files_activeRemove.contains(filename)) { // checks if file exists or in Remove INDEX
            outClient.println("ERROR_FILE_ALREADY_EXISTS");
//                                            ControllerLogger.getInstance().messageSent(client,
//                                                    Protocol.ERROR_FILE_ALREADY_EXISTS_TOKEN);
        } else {
            synchronized (lock) {
                if (files_activeStore.contains(filename)) { // INDEX CHECKS FOR CONCURENT FILE STORE
                    outClient.println("ERROR_FILE_ALREADY_EXISTS");
//                                                    ControllerLogger.getInstance().messageSent(client,
//                                                            Protocol.ERROR_FILE_ALREADY_EXISTS_TOKEN);
                    //continue;
                } else {
                    while (activeRebalance) { // waiting for rebalance to end
                        continue;
                    }
                    files_activeStore.add(filename);// ADD FILE STORING INDEX
                }
            }

            String portsToStore[] = getPortsToStore(R);
            String portsToStoreString = String.join(" ", portsToStore);
            fileToStore_ACKPorts.put(filename, new ArrayList<Integer>());// initialize store file acks
            outClient.println("STORE_TO" + " " + portsToStoreString);
//                                            ControllerLogger.getInstance().messageSent(client,
//                                                    Protocol.STORE_TO_TOKEN + " " + portsToStoreString);

            boolean success_Store = false;
            long timeout_time = System.currentTimeMillis() + timeout;
            while (System.currentTimeMillis() <= timeout_time) {
                if (fileToStore_ACKPorts.get(filename).size() >= R) { // checks if file to store has completed acknowledgements
                    outClient.println("STORE_COMPLETE");
//                                                    ControllerLogger.getInstance().messageSent(client,
//                                                            Protocol.STORE_COMPLETE_TOKEN);
                    dstore_file_ports.put(filename, fileToStore_ACKPorts.get(filename)); // update dstore_file_ports
                    file_filesize.put(filename, filesize); // add new file's filesize
                    success_Store = true;
                    break;
                }
            }

            if (!success_Store) {
                System.err.println("Store timed out for: " + filename);
            }

            synchronized (storeLock) {
                fileToStore_ACKPorts.remove(filename); // remove stored file from fileToStore_ACKPorts queue
            }
            files_activeStore.remove(filename);// FILE STORED REMOVE INDEX
            }
        }

    public void startController() {
        try {
            ServerSocket ss = new ServerSocket(cport);

            new Thread(() -> { // REBALANCE OPERATION THREAD
                while (true) {
                    this.rebalanceTime = System.currentTimeMillis() + rebalance_period;
                    while (System.currentTimeMillis() <= this.rebalanceTime) {
                        continue;
                    }
                    rebalanceOperation();
                    activeRebalance = false;
                }
            }).start();

            for (;;) {
                try {
                    System.out.println("Waiting for connection");
                    Socket client = ss.accept();

                    new Thread(() -> {
                        boolean isDstore = false;
                        Integer dstoreport = 0;
                        try {
                            System.out.println("Connected");
                            BufferedReader inClient = new BufferedReader(
                                    new InputStreamReader(client.getInputStream()));
                            PrintWriter outClient = new PrintWriter(client.getOutputStream(), true);
                            ConcurrentHashMap<String, ArrayList<Integer>> dstore_file_portsLeftReload = new ConcurrentHashMap<String, ArrayList<Integer>>();
                            String dataline = null;
                            for (;;) {
                                //-----------------------------Waiting for data-----------------------------
                                dataline = inClient.readLine();
//                                ControllerLogger.getInstance().messageReceived(client, dataline); // log recieved messages
                                if (dataline != null) {
                                    String[] data = dataline.split(" ");

                                    System.out.println("COMMAND RECIEVED \"" + getCommand(dataline) + "\"");

                                    //-----------------------------Client Store Command-----------------------------
                                    if (getCommand(dataline).equals("STORE")) {
                                        store(data, outClient);
                                    } else

                                        //-----------------------------Dstore Store_ACK Recieved-----------------------------
                                        if (getCommand(dataline).equals("STORE_ACK")) {
                                            synchronized (storeLock) {
                                                if (fileToStore_ACKPorts.containsKey(data[1]))
                                                    fileToStore_ACKPorts.get(data[1]).add(dstoreport);// add ack port inside chmap
                                            }
                                            dstore_port_files.get(dstoreport).add(data[1]);
                                            dstore_port_numbfiles.put(dstoreport,
                                                    dstore_port_numbfiles.get(dstoreport) + 1);

                                        } else

                                            //-----------------------------Dstore Remove_ACK Recieved-----------------------------
                                            if ((getCommand(dataline).equals("REMOVE_ACK")
                                                    || getCommand(dataline).equals("ERROR_FILE_DOES_NOT_EXIST"))) {
                                                removeACK(data, dstoreport);
                                            } else

                                            if (getCommand(dataline).equals("REBALANCE_COMPLETE") && activeRebalance) { // Dstore REMOVE_ACK filename
                                                rebalanceCompleteACK.incrementAndGet();
                                            } else

                                                //-----------------------------Client Load Command-----------------------------
                                                if (getCommand(dataline).equals("LOAD") || getCommand(dataline).equals("RELOAD")) {
                                                    load(data, outClient, dataline, dstore_file_portsLeftReload);
                                                } else

                                                    //-----------------------------Client Remove Command-----------------------------
                                                    if (getCommand(dataline).equals("REMOVE")) {
                                                        remove(data, outClient);
                                                    } else

                                                        //-----------------------------Dstore List Recieved-----------------------------
                                                        if (getCommand(dataline).equals("LIST") && isDstore && activeList) {
                                                            dstoreListReceived(dstoreport, data, client);
                                                        } else

                                                            //-----------------------------Client List Command-----------------------------
                                                            if (getCommand(dataline).equals("LIST") && !isDstore) {
                                                                getList(data, outClient);
                                                            } else

                                                                //-----------------------------Dstore Join Command-----------------------------
                                                                if (getCommand(dataline).equals("JOIN")) {
                                                                    if (data.length != 2) {
                                                                        System.err.println("Malformed message for Dstore Joining");
                                                                        continue;
                                                                    } // log error and continue
                                                                    dstoreport = Integer.parseInt(data[1]);
                                                                    if (dstore_port_numbfiles.containsKey(dstoreport)) { // checks if dstore port is already used
                                                                        System.err.println(
                                                                                "Connection refused, DStore port already used: " + dstoreport);
                                                                        client.close();
                                                                        break;
                                                                    }
                                                                    synchronized (DstoreJoinLock) {
                                                                        while (activeRebalance) {
                                                                            continue;
                                                                        }
//                                                                        ControllerLogger.getInstance().dstoreJoined(client, dstoreport);
                                                                        dstore_port_files.put(dstoreport, new ArrayList<String>()); // initialize port number of dstore
                                                                        dstore_port_numbfiles.put(dstoreport, 0); // initialize port/numbfiles hashmap
                                                                        dstore_port_Socket.put(dstoreport, client);
                                                                        isDstore = true;
                                                                        Dstore_count.incrementAndGet();
                                                                        activeRebalance = true;
                                                                        this.rebalanceTime = System.currentTimeMillis(); // turn on rebalance if not running
                                                                    }
                                                                } else {
                                                                    System.err.println("Unrecognised or Timed Out Command! - " + dataline);
                                                                    continue; // log error
                                                                }
                                } else {
                                    if (isDstore) {
                                        System.err.println("DSTORE Disconnected!");
                                        Dstore_count.decrementAndGet(); //decrease count if dstore disconnected
                                        while (activeRebalance) {
                                            continue;
                                        }
                                        synchronized (lock) {
                                            clearPort(dstoreport); // clear port data if dstore disonnected
                                        }
                                    }
                                    client.close();
                                    break;
                                }
                            }
                        } catch (Exception e) {
                            if (isDstore) {
                                System.err.println("DSTORE CRASHED! -" + e);
                                Dstore_count.decrementAndGet(); //decrease count if dstore disconnected
                                while (activeRebalance) {
                                    continue;
                                }
                                synchronized (lock) {
                                    clearPort(dstoreport); // clear port data if dstore disonnected
                                }
                            }
                            System.err.println("Fatal error in client: " + e);
                        }
                    }).start();
                } catch (Exception e) {
                    System.err.println("Could not create Socket: " + e);
                }
            }
        } catch (Exception e) {
            System.err.println("Could not create server socket: " + e);
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