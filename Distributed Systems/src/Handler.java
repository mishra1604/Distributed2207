import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.ConcurrentHashMap;

public class Handler implements Runnable {
    private Socket clientSocket;
    public ControllerObject1 controllerObject1 = null;
    public ArrayList<String> instructionList = new ArrayList<>();


    // Constructor
    public Handler(Socket socket, ControllerObject1 controllerObject1)
    {
        this.clientSocket = socket;
        this.controllerObject1 = controllerObject1;
    }


    public static void main(String[] args) {

    }

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

    public void dstoreList (String[] data, Integer dstoreport) {
        ArrayList<String> filelist = new ArrayList<String>(Arrays.asList(data));
        filelist.remove(0); // remove command entry
        controllerObject1.getObject().getDstore_port_numbfiles().put(dstoreport, filelist.size()); // updates port/numbfiles hashmap
        controllerObject1.getObject().getDstore_port_files().put(dstoreport, filelist); // puts list in hashmap
        controllerObject1.getObject().getDstore_port_Socket().put(dstoreport, clientSocket);
        for (String string : filelist) {
            if (controllerObject1.getObject().getDstore_file_ports().get(string) == null) {
                controllerObject1.getObject().getDstore_file_ports().put(string, new ArrayList<Integer>());
            }
            if (!controllerObject1.getObject().getDstore_file_ports().get(string).contains(dstoreport))
                controllerObject1.getObject().getDstore_file_ports().get(string).add(dstoreport); // puts the given file the port that its in
        }
        controllerObject1.getObject().getListACKPorts().add(dstoreport);
    }


    public void listForClient (String[] data, PrintWriter outClient){
        for (;;) {
            if (data.length != 1) {
                System.err.println("Malformed message received for LIST by Client");
                continue;
            } // log error and continue
            if (controllerObject1.getObject().getDstoreCount().get() < controllerObject1.getObject().getR()) {
                outClient.println("ERROR_LOAD");
            } else if (controllerObject1.getObject().getFile_filesize().size() != 0) {
                String filesList = String.join(" ", controllerObject1.getObject().getFile_filesize().keySet());
                outClient.println("LIST" + " " + filesList);
            } else {
                outClient.println("LIST");
            }
            break;
        }
    }


    public void removeFile(String[] data, PrintWriter outClient) {
        for (;;) {
            if (data.length != 2) {
                System.err.println("Malformed message received for REMOVE");
                continue;
            } // log error and continue
            String filename = data[1];
            if (controllerObject1.getObject().getDstoreCount().get() < controllerObject1.getObject().getR()) {
                outClient.println("ERROR_NOT_ENOUGH_DSTORES");
//                                                            ControllerLogger.getInstance().messageSent(client,
//                                                                    Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
            } else if (!controllerObject1.getObject().getFile_filesize().containsKey(filename)
                    || controllerObject1.getObject().getFiles_activeStore().contains(filename)) {
                outClient.println("ERROR_FILE_DOES_NOT_EXIST");
//                                                            ControllerLogger.getInstance().messageSent(client,
//                                                                    Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
            } else {
                synchronized (controllerObject1.getObject().getLock()) {
                    if (controllerObject1.getObject().getFiles_activeRemove().contains(filename)
                            || !controllerObject1.getObject().getFile_filesize().containsKey(filename)) { // INDEX CHECKS FOR CONCURENT FILE STORE
                        outClient.println("ERROR_FILE_DOES_EXIST");
//                                                                    ControllerLogger.getInstance().messageSent(client,
//                                                                            Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
                        continue;
                    } else {
                        /*while (controllerObject1.getObject().getActiveRebalance()) { // waiting for rebalance to end
                            continue;
                        }*/
                        controllerObject1.getObject().getFiles_activeRemove().add(filename);
                        controllerObject1.getObject().getFile_filesize().remove(filename);// remove file_filesize so if broken rebalance should fix
                    }
                }
                controllerObject1.getObject().getFileToRemove_ACKPorts().put(filename,
                        new ArrayList<>(controllerObject1.getObject().getDstore_file_ports().get(filename))); // initializes the ports that wait for remove
                controllerObject1.getObject().getFiles_addCount().remove(filename);

                synchronized (controllerObject1.getObject().getRemoveLock()) {
                    for (Integer port : controllerObject1.getObject().getFileToRemove_ACKPorts().get(filename)) { // send ports file to delete
                        Socket dstoreSocket = controllerObject1.getObject().getDstore_port_Socket().get(port);
                        PrintWriter outDstore = null;
                        try {
                            outDstore = new PrintWriter(
                                    dstoreSocket.getOutputStream(), true);
                        } catch (IOException e) {
                            System.out.println("outDstore error, hanldeThings()" + e);
                        }
                        outDstore.println("REMOVE" + " " + filename);
//                                                                    ControllerLogger.getInstance().messageSent(dstoreSocket,
//                                                                            Protocol.REMOVE_TOKEN + " " + filename);
                    }
                }

                boolean success_Remove = false;
                long timeout_time = System.currentTimeMillis() + controllerObject1.getObject().getTimeout();
                while (System.currentTimeMillis() <= timeout_time) {
                    if (controllerObject1.getObject().getFileToRemove_ACKPorts().get(filename).size() == 0) { // checks if file to store has completed acknowledgements
                        outClient.println("REMOVE_COMPLETE");
//                                                                    ControllerLogger.getInstance().messageSent(client,
//                                                                            Protocol.REMOVE_COMPLETE_TOKEN);
                        controllerObject1.getObject().getDstore_file_ports().remove(filename);
                        success_Remove = true;
                        break;
                    }
                }

                if (!success_Remove) {
                    System.err.println("REMOVE timed out for: " + filename);
                }
                controllerObject1.getObject().getFileToRemove_ACKPorts().remove(filename);
                controllerObject1.getObject().getFiles_activeRemove().remove(filename); // remove file ActiveRemove from INDEX
            }
            break;
        }
    }

    public void load (String[] data, PrintWriter outClient, String dataline, ConcurrentHashMap<String, ArrayList<Integer>> dstore_file_portsLeftReload) {
        for (;;) {
            if (data.length != 2) {
                System.err.println("Malformed message received for LOAD/RELOAD");
                continue;
            } // log error and continue
            String filename = data[1];
            if (controllerObject1.getObject().getDstoreCount().get() < controllerObject1.getObject().getR()) {
                outClient.println("ERROR_NOT_ENOUGH_DSTORES");
//                                                        ControllerLogger.getInstance().messageSent(client,
//                                                                Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
            } else {
                if (!controllerObject1.getObject().getFile_filesize().containsKey(filename)) { // CHECKS FILE CONTAINS AND INDEX
                    outClient.println("ERROR_FILE_DOES_NOT_EXIST");
//                                                            ControllerLogger.getInstance().messageSent(client,
//                                                                    Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
                } else {
                    if (controllerObject1.getObject().getFiles_activeStore().contains(filename)
                            || controllerObject1.getObject().getFiles_activeRemove().contains(filename)) { // INDEX CHECKS FOR CONCURENT FILE STORE
                        if (getCommand(dataline).equals("LOAD")) {
                            outClient.println("ERROR_FILE_DOES_NOT_EXIST");
//                                                                    ControllerLogger.getInstance().messageSent(client,
//                                                                            Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
                            continue;
                        } else {
                            outClient.println("ERROR_LOAD");
//                                                                    ControllerLogger.getInstance().messageSent(client,
//                                                                            Protocol.ERROR_LOAD_TOKEN);
                            continue;
                        }
                    }

                    /*while (controllerObject1.getObject().getActiveRebalance()) {
                        continue;
                    }*/

                    if (getCommand(dataline).equals("LOAD")) {
                        dstore_file_portsLeftReload.put(filename,
                                new ArrayList<>(controllerObject1.getObject().getDstore_file_ports().get(filename)));
                        outClient.println("LOAD_FROM" + " "
                                + dstore_file_portsLeftReload.get(filename).get(0) + " "
                                + controllerObject1.getObject().getFile_filesize().get(filename));
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
                                    + controllerObject1.getObject().getFile_filesize().get(filename));
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
            break;
        }
    }

    public void store (String[] data, PrintWriter outClient) {
        for (;;) {
            if (data.length != 3) {
                System.err.println("Malformed message received for STORE");
                continue;
            } // log error and continue
            String filename = data[1];
            Integer filesize = Integer.parseInt(data[2]);

            if (controllerObject1.getObject().getDstoreCount().get() < controllerObject1.getObject().getR()) {
                outClient.println("ERROR_NOT_ENOUGH_DSTORES");
//                                            ControllerLogger.getInstance().messageSent(client,
//                                                    Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
            } else if (controllerObject1.getObject().getFile_filesize().get(filename) != null
                    || controllerObject1.getObject().getFile_filesize().contains(filename)) { // checks if file exists or in Remove INDEX
                outClient.println("ERROR_FILE_ALREADY_EXISTS");
//                                            ControllerLogger.getInstance().messageSent(client,
//                                                    Protocol.ERROR_FILE_ALREADY_EXISTS_TOKEN);
            } else {
                synchronized (controllerObject1.getObject().getLock()) {
                    if (controllerObject1.getObject().getFiles_activeStore().contains(filename)) { // INDEX CHECKS FOR CONCURENT FILE STORE
                        outClient.println("ERROR_FILE_ALREADY_EXISTS");
//                                                    ControllerLogger.getInstance().messageSent(client,
//                                                            Protocol.ERROR_FILE_ALREADY_EXISTS_TOKEN);
                        continue;
                    } else {
                        /*while (controllerObject1.getObject().getActiveRebalance()) { // waiting for rebalance to end
                            continue;
                        }*/
                        controllerObject1.getObject().getFiles_activeStore().add(filename);// ADD FILE STORING INDEX
                    }
                }

                String portsToStore[] = getPortsToStore(controllerObject1.getObject().getR());
                String portsToStoreString = String.join(" ", portsToStore);
                controllerObject1.getObject().getFileToStore_ACKPorts().put(filename, new ArrayList<Integer>());// initialize store file acks
                outClient.println("STORE_TO" + " " + portsToStoreString);
//                                            ControllerLogger.getInstance().messageSent(client,
//                                                    Protocol.STORE_TO_TOKEN + " " + portsToStoreString);

                boolean success_Store = false;
                long timeout_time = System.currentTimeMillis() + controllerObject1.getObject().getTimeout();
                while (System.currentTimeMillis() <= timeout_time) {
                    if (controllerObject1.getObject().getFileToStore_ACKPorts().get(filename).size() >= controllerObject1.getObject().getR()) { // checks if file to store has completed acknowledgements
                        outClient.println("STORE_COMPLETE");
//                                                    ControllerLogger.getInstance().messageSent(client,
//                                                            Protocol.STORE_COMPLETE_TOKEN);
                        controllerObject1.getObject().getDstore_file_ports().put(filename, controllerObject1.getObject().getFileToStore_ACKPorts().get(filename)); // update dstore_file_ports
                        controllerObject1.getObject().getFile_filesize().put(filename, filesize); // add new file's filesize
                        success_Store = true;
                        break;
                    }
                }

                if (!success_Store) {
                    System.err.println("Store timed out for: " + filename);
                }

                synchronized (controllerObject1.getObject().getStoreLock()) {
                    controllerObject1.getObject().getFileToStore_ACKPorts().remove(filename); // remove stored file from fileToStore_ACKPorts queue
                }
                controllerObject1.getObject().getFiles_activeStore().remove(filename);// FILE STORED REMOVE INDEX
            }
            break;
        }
    }

    public void dstoreJoin (String[] data, Integer dstoreport, boolean isDstore) {
        for (;;) {
            if (data.length != 2) {
                System.err.println("Malformed message for Dstore Joining");
                continue;
            } // log error and continue
            dstoreport = Integer.parseInt(data[1]);
            if (controllerObject1.getObject().getDstore_port_numbfiles().containsKey(dstoreport)) { // checks if dstore port is already used
                System.err.println(
                        "Connection refused, DStore port already used: " + dstoreport);
                try {
                    clientSocket.close();
                } catch (IOException e) {
                    System.out.println("Error in clientSocket.close() handleThings() " + e);
                }
                break;
            }
            synchronized (controllerObject1.getObject().getDstoreJoinLock()) {
                /*while (controllerObject1.getObject().getActiveRebalance()) {
                    continue;
                }*/
//                                                                        ControllerLogger.getInstance().dstoreJoined(client, dstoreport);
                controllerObject1.getObject().getDstore_port_files().put(dstoreport, new ArrayList<String>()); // initialize port number of dstore
                controllerObject1.getObject().getDstore_port_numbfiles().put(dstoreport, 0); // initialize port/numbfiles hashmap
                controllerObject1.getObject().getDstore_port_Socket().put(dstoreport, clientSocket);
                isDstore = true;
                controllerObject1.getObject().getDstoreCount().incrementAndGet();
                //controllerObject1.getObject().setActiveRebalance(true);
                //controllerObject1.getObject().setRebalanceTime(System.currentTimeMillis()); // turn on rebalance if not running
            }
        }
    }

    public void storeACK (String[] data, Integer dstoreport) {
        synchronized (controllerObject1.getObject().getStoreLock()) {
            if (controllerObject1.getObject().getFileToStore_ACKPorts().containsKey(data[1]))
                controllerObject1.getObject().getFileToStore_ACKPorts().get(data[1]).add(dstoreport);// add ack port inside chmap
        }
        controllerObject1.getObject().getDstore_port_files().get(dstoreport).add(data[1]);
        controllerObject1.getObject().getDstore_port_numbfiles().put(dstoreport,
                controllerObject1.getObject().getDstore_port_numbfiles().get(dstoreport) + 1);
    }

    public void removeACK (String[] data, Integer dstoreport) {
        synchronized (controllerObject1.getObject().getRemoveLock()) {
            if (controllerObject1.getObject().getFileToRemove_ACKPorts().containsKey(data[1])) {
                controllerObject1.getObject().getFileToRemove_ACKPorts().get(data[1]).remove(dstoreport);
            } // removing dstore with ack from list
        }
        controllerObject1.getObject().getDstore_port_files().get(dstoreport).remove(data[1]); //removes file from map of port
        controllerObject1.getObject().getDstore_file_ports().get(data[1]).remove(dstoreport);// remove port from file - ports map
        controllerObject1.getObject().getDstore_port_numbfiles().put(dstoreport,
                controllerObject1.getObject().getDstore_port_numbfiles().get(dstoreport) - 1); // suspend 1 from file count
    }

    public ArrayList<String> getInstructionList() {
        instructionList.add("STORE");
        instructionList.add("STORE_ACK");
        instructionList.add("REMOVE_ACK");
        instructionList.add("ERROR_FILE_DOES_NOT_EXIST");
        instructionList.add("LOAD");
        instructionList.add("RELOAD");
        instructionList.add("REMOVE");
        instructionList.add("REMOVE");
        instructionList.add("LIST");
        return instructionList;
    }

    public void instructionHandler (String command, String[] data, String dataline, PrintWriter outClient, Integer dstoreport, ConcurrentHashMap<String, ArrayList<Integer>> dstore_file_portsLeftReload, boolean isDstore) {
        //-----------------------------Client Store Command-----------------------------
        if (command.equals("STORE")) {
            store(data, outClient);
            //-----------------------------Dstore Store_ACK Recieved-----------------------------
        } else if (command.equals("STORE_ACK")) {
            storeACK(data, dstoreport);
            //-----------------------------Dstore Remove_ACK Recieved-----------------------------
        } else if (command.equals("REMOVE_ACK") || command.equals("ERROR_FILE_DOES_NOT_EXIST")) {
            removeACK(data, dstoreport);
            //-----------------------------Client Load Command-----------------------------
        } else if (command.equals("LOAD") || command.equals("RELOAD")) {
            load(data, outClient, dataline, dstore_file_portsLeftReload);
            //-----------------------------Client Remove Command-----------------------------
        } else if (command.equals("REMOVE")) {
            removeFile(data, outClient);
            //-----------------------------Client List Command-----------------------------
        } else if (command.equals("LIST") && !isDstore) {
            listForClient(data, outClient);

        }
    }


    public void handleThings(String dataline, BufferedReader inClient, PrintWriter outClient, Integer dstoreport, ConcurrentHashMap<String, ArrayList<Integer>> dstore_file_portsLeftReload, boolean isDstore) {
        for (;;) {
            //-----------------------------Waiting for data-----------------------------
            try {
                dataline = inClient.readLine();
            } catch (IOException e) {
                e.printStackTrace();
                System.out.println("dataline readLine error, hadleThings()" + e);
            }
            if (dataline != null) {
                String[] data = dataline.split(" ");
                getCommand(dataline);

                System.out.println("RECEIVED INSTRUCTION =  \"" + getCommand(dataline) + "\"");

                if (getInstructionList().contains(getCommand(dataline))) {
                    instructionHandler(getCommand(dataline), data, dataline, outClient, dstoreport, dstore_file_portsLeftReload, isDstore);

                } /*else if (getCommand(dataline).equals("REBALANCE_COMPLETE") && controllerObject1.getObject().getActiveRebalance()) { // Dstore REMOVE_ACK filename
                            controllerObject1.getObject().getRebalanceCompleteACK().incrementAndGet();

                }*/  else if (getCommand(dataline).equals("LIST") && isDstore && controllerObject1.getObject().getActiveList()) {
                    dstoreList(data, dstoreport);

                }  else if (getCommand(dataline).equals("JOIN")) {
                    if (data.length != 2) {
                        System.err.println("Malformed message for Dstore Joining");
                        continue;
                    } // log error and continue
                    dstoreport = Integer.parseInt(data[1]);
                    if (controllerObject1.getObject().getDstore_port_numbfiles().containsKey(dstoreport)) { // checks if dstore port is already used
                        System.err.println(
                                "Connection refused, DStore port already used: " + dstoreport);
                        try {
                            clientSocket.close();
                        } catch (IOException e) {
                            System.out.println("Error in clientSocket.close() handleThings() " + e);
                        }
                        break;
                    }
                    synchronized (controllerObject1.getObject().getDstoreJoinLock()) {
                        while (controllerObject1.getObject().getActiveRebalance()) {
                            continue;
                        }
                        controllerObject1.getObject().getDstore_port_files().put(dstoreport, new ArrayList<String>()); // initialize port number of dstore
                        controllerObject1.getObject().getDstore_port_numbfiles().put(dstoreport, 0); // initialize port/numbfiles hashmap
                        controllerObject1.getObject().getDstore_port_Socket().put(dstoreport, clientSocket);
                        isDstore = true;
                        controllerObject1.getObject().getDstoreCount().incrementAndGet();
                        //controllerObject1.getObject().setActiveRebalance(true);
                        //controllerObject1.getObject().setRebalanceTime(System.currentTimeMillis()); // turn on rebalance if not running
                    }
                } else {
                    System.err.println("Unrecognised or Timed Out Command! - " + dataline);
                    continue; // log error
                }
            } else {
                if (isDstore) {
                    System.err.println("DSTORE Disconnected!");
                    controllerObject1.getObject().getDstoreCount().decrementAndGet(); //decrease count if dstore disconnected
                    /*while (controllerObject1.getObject().getActiveRebalance()) {
                        continue;
                    }*/
                    synchronized (controllerObject1.getObject().getLock()) {
                        clearPort(dstoreport); // clear port data if dstore disonnected
                    }
                }
                try {
                    clientSocket.close();
                } catch (IOException e) {
                    System.out.println("Error in closing client handleThings()" + e);
                }
                break;
            }
        }

    }

    @Override
    public void run() {
        try {
            boolean isDstore = false;
            Integer dstoreport = 0;
            try {
                System.out.println("Connected");
                BufferedReader inClient = new BufferedReader(
                        new InputStreamReader(clientSocket.getInputStream()));
                PrintWriter outClient = new PrintWriter(clientSocket.getOutputStream(), true);
                ConcurrentHashMap<String, ArrayList<Integer>> dstore_file_portsLeftReload = new ConcurrentHashMap<String, ArrayList<Integer>>();
                String dataline = null;
                handleThings(dataline, inClient, outClient, dstoreport, dstore_file_portsLeftReload, isDstore);
            } catch (Exception e) {
                if (isDstore) {
                    System.err.println("DSTORE CRASHED! -" + e);
                    controllerObject1.getObject().getDstoreCount().decrementAndGet(); //decrease count if dstore disconnected
                    /*while (controllerObject1.getObject().getActiveRebalance()) {
                        continue;
                    }*/
                    synchronized (controllerObject1.getObject().getLock()) {
                        clearPort(dstoreport); // clear port data if dstore disonnected
                    }
                }
                System.err.println("Fatal error in client: " + e);
            }
        } catch (Exception e) {
            System.out.println("failed method" + e);
        }
    }

    private String[] getPortsToStore(int R) { // finds R ports with least files
        Integer ports[] = new Integer[R];

        for (Integer port : controllerObject1.getObject().getDstore_port_numbfiles().keySet()) {
            int max = 0;

            for (int i = 0; i < R; i++) {
                if (ports[i] == null) {
                    max = i;
                    ports[i] = port;
                    break;
                }
                if (ports[i] != null && controllerObject1.getObject().getDstore_port_numbfiles().get(ports[i]) > controllerObject1.getObject().getDstore_port_numbfiles().get(ports[max])) {
                    max = i;
                }
            }
            if (controllerObject1.getObject().getDstore_port_numbfiles().get(port) < controllerObject1.getObject().getDstore_port_numbfiles().get(ports[max])) {
                ports[max] = port;
            }
        }

        String returnPorts[] = new String[R];
        for (int i = 0; i < R; i++) {
            returnPorts[i] = ports[i].toString();
        }
        return returnPorts;
    }

    private synchronized void clearPort(Integer port) {
        System.out.println("CLEARING DISCONNECTED PORT " + port);
        for (String file : controllerObject1.getObject().getDstore_port_files().get(port)) {
            if (controllerObject1.getObject().getFiles_addCount().get(file) == null) {
                controllerObject1.getObject().getFiles_addCount().put(file, 1);
            } else {
                controllerObject1.getObject().getFiles_addCount().put(file, controllerObject1.getObject().getFiles_addCount().get(file) + 1);
            }
        }
        controllerObject1.getObject().getDstore_port_files().remove(port);
        controllerObject1.getObject().getDstore_port_numbfiles().remove(port);
        controllerObject1.getObject().getDstore_port_Socket().remove(port);
        ConcurrentHashMap<String, ArrayList<Integer>> tempFilePorts = new ConcurrentHashMap<String, ArrayList<Integer>>(
                controllerObject1.getObject().getDstore_file_ports());
        for (String file : tempFilePorts.keySet()) {
            if (!controllerObject1.getObject().getFile_filesize().keySet().contains(file)) {
                controllerObject1.getObject().getDstore_file_ports().remove(file);
            } else if (controllerObject1.getObject().getDstore_file_ports().get(file).contains(port)) {
                controllerObject1.getObject().getDstore_file_ports().get(file).remove(port);
            }
        }
        System.out.println("CLEARED PORT " + port);
    }
}
