import java.io.*;
import java.net.InetAddress;
import java.net.Socket;

public class HandleDstore implements Runnable{

    Socket controller;
    SubDstore subDstoreClass = null;

    public HandleDstore (Socket socket, SubDstore subDstoreClass) {
        this.controller = socket;
        this.subDstoreClass = subDstoreClass;
    }

    @Override
    public void run() {
        try {
            BufferedReader inController = new BufferedReader(new InputStreamReader(controller.getInputStream()));
            PrintWriter outController = new PrintWriter(controller.getOutputStream(), true);
            String dataline = null;

            outController.println("JOIN" + " " + subDstoreClass.getObject().getPort());
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
                            File fileRemove = new File(subDstoreClass.getObject().getPath() + File.separator + filename);
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
                                        File existingFile = new File(subDstoreClass.getObject().getPath() + File.separator + filename);
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
                                    File existingFile = new File(subDstoreClass.getObject().getPath() + File.separator + data[z]);
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
                                    String[] fileList = subDstoreClass.getObject().getFolder().list();
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
                        subDstoreClass.getObject().setController_fail(true);
                        break;
                    }
                }
            } catch (Exception e) {
                System.err.println("Controller Disconnected Error: " + e);
                if (controller.isConnected())
                    controller.close();
                subDstoreClass.getObject().setController_fail(true);
            }
        } catch (Exception e) {
            System.err.println("Initial Controller Connection Error: " + e);
            subDstoreClass.getObject().setController_fail(true);
        }
    }
}
