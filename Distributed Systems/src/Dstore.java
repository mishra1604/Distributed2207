import java.io.*;
import java.net.*;

public class Dstore {
    public Integer getPort() {
        return port;
    }

    private Integer port;
    private Integer cport;

    public Integer getTimeout() {
        return timeout;
    }

    private Integer timeout;
    private String file_folder;

    public boolean isController_fail() {
        return controller_fail;
    }

    public void setController_fail(boolean controller_fail) {
        this.controller_fail = controller_fail;
    }

    private boolean controller_fail = false;

    public String getPath() {
        return path;
    }

    String path;

    public File getFolder() {
        return folder;
    }

    File folder;
    SubDstore subDstore;

    public Dstore(int port, int cport, int timeout, String file_folder) {
        this.port = port;
        this.cport = cport;
        this.timeout = timeout;
        this.file_folder = file_folder;
//        try {
//            //DstoreLogger.init(Logger.LoggingType.ON_FILE_AND_TERMINAL, port);
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
        try {
            //startDstore();
            openDstore();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void openDstore() throws IOException {
        folder = new File(file_folder); // folder to make
        if (!folder.exists())
            if (!folder.mkdir())
                throw new RuntimeException("Cannot create new folder in " + folder.getAbsolutePath());
            path = folder.getAbsolutePath(); // path of folder
        for (File file : folder.listFiles()) // delete every file in directory if any
        {
            file.delete();
        }
        Socket controller = new Socket(InetAddress.getByName("localhost"), cport);
        System.out.println("DSTORE PORET: " + port);
        try {

            subDstore = new SubDstore(this);
            HandleDstore dstoreHandler = new HandleDstore(controller, subDstore);
            new Thread(dstoreHandler).start();

            if (controller_fail)
                return;


            try {
                ServerSocket ss = new ServerSocket(port);
                for (;;) {
                    System.out.println("Client waiting");
                    Socket client = ss.accept();

                    DstoreClientHandle clientHandler = new DstoreClientHandle(client, controller, subDstore);
                    new Thread(clientHandler).start();
                }
            } catch (Exception e) {
                System.out.println("Second Thread - Client " + e);
            }

        } catch (Exception e) {
            System.out.println("openDstore() " + e);
        }
    }


    public void startDstore() throws IOException {
        folder = new File(file_folder); // folder to make
        if (!folder.exists())
            if (!folder.mkdir())
                throw new RuntimeException("Cannot create new folder in " + folder.getAbsolutePath());
        final String path = folder.getAbsolutePath(); // path of folder
        for (File file : folder.listFiles()) // delete every file in directory if any
        {
            file.delete();
        }

        Socket controller = new Socket(InetAddress.getByName("localhost"), cport);
        System.out.println("DSTORE PORET: " + port);
        new Thread(() -> { // CONTROLLER
            try {
                BufferedReader inController = new BufferedReader(new InputStreamReader(controller.getInputStream()));
                PrintWriter outController = new PrintWriter(controller.getOutputStream(), true);
                String dataline = null;

                outController.println("JOIN" + " " + port);
                //DstoreLogger.getInstance().messageSent(controller, "JOIN" + " " + port);
                System.out.println("Entering loop of Controller");

                try {
                    for (;;) {
                        dataline = inController.readLine();
                        //DstoreLogger.getInstance().messageReceived(controller, dataline);
                        if (dataline != null) {
                            String[] data = dataline.split(" ");
                            String command;
                            if (data.length == 1) {
                                command = dataline.trim();
                                data[0] = command;
                                dataline = null;
                            } else {
                                command = data[0];
                                data[data.length - 1] = data[data.length - 1].trim();
                                dataline = null;
                            }
                            System.out.println("Revieved Controller Command: " + command);

                            //-----------------------------Controller Remove Command-----------------------------
                            if (command.equals("REMOVE")) {
                                if (data.length != 2) {
                                    System.err.println("Malformed message received for Remove");
                                    continue;
                                } // log error and continue
                                String filename = data[1];
                                File fileRemove = new File(path + File.separator + filename);
                                if (!fileRemove.exists() || !fileRemove.isFile()) {
                                    outController.println("ERROR_FILE_DOES_NOT_EXIST" + " " + filename);
//                                    DstoreLogger.getInstance().messageSent(controller,
//                                            Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN + " " + filename);
                                } else {
                                    fileRemove.delete();
                                    outController.println("REMOVE_ACK" + " " + filename);
//                                    DstoreLogger.getInstance().messageSent(controller,
//                                            Protocol.REMOVE_ACK_TOKEN + " " + filename);
                                }
                            } else

                                //-----------------------------Controller Rebalance Command-----------------------------
                                if (command.equals("REBALANCE")) {
                                    Integer filesToSend = Integer.parseInt(data[1]);
                                    Integer index = 2;
                                    for (int i = 0; i < filesToSend; i++) {
                                        String filename = data[index];
                                        Integer portSendCount = Integer.parseInt(data[index + 1]);
                                        for (int j = index + 2; j <= index + 1 + portSendCount; j++) {
                                            Socket dStoreSocket = new Socket(InetAddress.getByName("localhost"),
                                                    Integer.parseInt(data[j]));
                                            BufferedReader inDstore = new BufferedReader(
                                                    new InputStreamReader(dStoreSocket.getInputStream()));
                                            PrintWriter outDstore = new PrintWriter(dStoreSocket.getOutputStream(), true);
                                            File existingFile = new File(path + File.separator + filename);
                                            Integer filesize = (int) existingFile.length(); // casting long to int file size limited to fat32
                                            outDstore.println(
                                                    "REBALANCE_STORE" + " " + filename + " " + filesize);
//                                            DstoreLogger.getInstance().messageSent(dStoreSocket,
//                                                    Protocol.REBALANCE_STORE_TOKEN + " " + filename + " " + filesize);
                                            if (inDstore.readLine() == "ACK") {
//                                                DstoreLogger.getInstance().messageReceived(dStoreSocket,
//                                                        Protocol.ACK_TOKEN);
                                                FileInputStream inf = new FileInputStream(existingFile);
                                                OutputStream out = dStoreSocket.getOutputStream();
                                                out.write(inf.readNBytes(filesize));
                                                out.flush();
                                                inf.close();
                                                out.close();
                                                dStoreSocket.close();
                                            } else {
                                                dStoreSocket.close();
                                            }
                                        }
                                        index = index + portSendCount + 2; // ready index for next file
                                    }
                                    Integer fileRemoveCount = Integer.parseInt(data[index]);
                                    for (int z = index + 1; z < index + 1 + fileRemoveCount; z++) {
                                        File existingFile = new File(path + File.separator + data[z]);
                                        if (existingFile.exists()) {
                                            existingFile.delete();
                                        }
                                    }

                                    outController.println("REBALANCE_COMPLETE");
//                                    DstoreLogger.getInstance().messageSent(controller, "REBALANCE_COMPLETE");

                                } else

                                    //-----------------------------Controller List Command-----------------------------
                                    if (command.equals("LIST")) {
                                        if (data.length != 1) {
                                            System.err.println("Malformed message received for LIST");
                                            continue;
                                        } // log error and continue
                                        String[] fileList = folder.list();
                                        String listToSend = String.join(" ", fileList);
                                        outController.println("LIST" + " " + listToSend);
//                                        DstoreLogger.getInstance().messageSent(controller,
//                                                Protocol.LIST_TOKEN + " " + listToSend);
                                    } else {
                                        System.err.println("Unrecognised command! - " + dataline); //log and continue
                                    }
                        } else {
                            if (controller.isConnected())
                                controller.close();
                            controller_fail = true;
                            break;
                        }
                    }
                } catch (Exception e) {
                    System.err.println("Controller Disconnected Error: " + e);
                    if (controller.isConnected())
                        controller.close();
                    controller_fail = true;
                }
            } catch (Exception e) {
                System.err.println("Initial Controller Connection Error: " + e);
                controller_fail = true;
            }
        }).start();

        if (controller_fail)
            return; //exit if controller connection had failed

        /* -----------------------------Clients Part-----------------------------*/
        try {
            ServerSocket ss = new ServerSocket(port);
            for (;;) {
                System.out.println("Client waiting");
                Socket client = ss.accept();
                new Thread(() -> { // CLIENTS
                    try {
                        PrintWriter outController = new PrintWriter(controller.getOutputStream(), true);
                        BufferedReader inClient = new BufferedReader(new InputStreamReader(client.getInputStream()));
                        PrintWriter outClient = new PrintWriter(client.getOutputStream(), true);
                        String dataline;
                        InputStream in = client.getInputStream();
                        System.out.println("Client Connected");

                        for (;;) {
                            try {
                                dataline = inClient.readLine();
//                                DstoreLogger.getInstance().messageReceived(client, dataline);
                                if (dataline != null) {
                                    String[] data = dataline.split(" ");
                                    String command;
                                    if (data.length == 1) {
                                        command = dataline.trim();
                                        data[0] = command;
                                    } else {
                                        command = data[0];
                                    }
                                    System.out.println("Recieved Client Command: " + command);

                                    //-----------------------------Client Store Command-----------------------------
                                    if (command.equals("STORE")) {
                                        if (data.length != 3) {
                                            System.err.println("Malformed message received for STORE");
                                            continue;
                                        } // log error and continue
                                        outClient.println("ACK");
//                                        DstoreLogger.getInstance().messageSent(client, "ACK");
                                        int filesize = Integer.parseInt(data[2]);
                                        File outputFile = new File(path + File.separator + data[1]);
                                        FileOutputStream out = new FileOutputStream(outputFile);
                                        long timeout_time = System.currentTimeMillis() + timeout;
                                        while (System.currentTimeMillis() <= timeout_time) {
                                            out.write(in.readNBytes(filesize)); // possible threadlock?? maybe
                                            outController.println("STORE_ACK" + " " + data[1]);
//                                            DstoreLogger.getInstance().messageSent(controller,
//                                                    Protocol.STORE_ACK_TOKEN + " " + data[1]);
                                            break;
                                        }
                                        out.flush();
                                        out.close();
                                        client.close();
                                        return;
                                    } else

                                        //-----------------------------Dstore Rebalance Asked-----------------------------
                                        if (command.equals("REBALANCE_STORE")) {
                                            if (data.length != 3) {
                                                System.err.println("Malformed message received for REBALANCE_STORE");
                                                continue;
                                            } // log error and continue
                                            outClient.println("ACK");
//                                            DstoreLogger.getInstance().messageSent(client, Protocol.ACK_TOKEN);
                                            int filesize = Integer.parseInt(data[2]);
                                            File outputFile = new File(path + File.separator + data[1]);
                                            FileOutputStream out = new FileOutputStream(outputFile);
                                            out.write(in.readNBytes(filesize)); // possible threadlock?? maybe
                                            out.flush();
                                            out.close();
                                            client.close();
                                            return;
                                        } else

                                            //-----------------------------Client Load Command-----------------------------
                                            if (command.equals("LOAD_DATA")) {
                                                if (data.length != 2) {
                                                    System.err.println("Malformed message received for LOAD");
                                                    continue;
                                                } // log error and continue
                                                String filename = data[1];
                                                File existingFile = new File(path + File.separator + filename);
                                                if (!existingFile.exists() || !existingFile.isFile()) {
                                                    client.close();
                                                    return;
                                                } // closes connection and exits thread

                                                int filesize = (int) existingFile.length(); // casting long to int file size limited to fat32
                                                FileInputStream inf = new FileInputStream(existingFile);
                                                OutputStream out = client.getOutputStream();
                                                out.write(inf.readNBytes(filesize));
                                                out.flush();
                                                inf.close();
                                                out.close();
                                                client.close();
                                                return;
                                            } else {
                                                System.err.println("Unrecognised Command! - " + dataline);
                                                continue; // log error
                                            }
                                } else {
                                    client.close();
                                    break;
                                }
                            } catch (Exception e) {
                                System.err.println("Client disconnected error: " + e);
                                client.close();
                                break;
                            }
                        }

                    } catch (Exception e) {
                        System.err.println("Fatal Dstore error: " + e);
                    }
                }).start();
            }
        } catch (Exception e) {
            System.err.println("Could not initialize Socket: " + e);
        }
    }

    public static void main(String[] args) throws IOException {
        int port = Integer.parseInt(args[0]);
        int cport = Integer.parseInt(args[1]);
        int timeout = Integer.parseInt(args[2]);
        String file_folder = args[3];
        Dstore dstore = new Dstore(port, cport, timeout, file_folder);
    }
}