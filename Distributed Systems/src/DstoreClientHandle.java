import java.io.*;
import java.net.Socket;

public class DstoreClientHandle implements Runnable {
    Socket client;
    Socket controller;
    SubDstore subDstore = null;

    public DstoreClientHandle (Socket clientsocket, Socket controllersocket, SubDstore subDstore) {
        this.client = clientsocket;
        this.controller = controllersocket;
        this.subDstore = subDstore;
    }


    @Override
    public void run() {
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
                            File outputFile = new File(subDstore.getObject().getPath() + File.separator + data[1]);
                            FileOutputStream out = new FileOutputStream(outputFile);
                            long timeout_time = System.currentTimeMillis() + subDstore.getObject().getTimeout();
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
                                File outputFile = new File(subDstore.getObject().getPath() + File.separator + data[1]);
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
                                    File existingFile = new File(subDstore.getObject().getPath() + File.separator + filename);
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
    }
}
