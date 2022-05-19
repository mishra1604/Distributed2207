import java.io.*;
import java.net.InetAddress;
import java.net.Socket;
import java.util.ArrayList;

public class HandleDstore implements Runnable{

    Socket controller;
    SubDstore subDstoreClass = null;

    ArrayList<String> instructionList = new ArrayList<>();

    public HandleDstore (Socket socket, SubDstore subDstoreClass) {
        this.controller = socket;
        this.subDstoreClass = subDstoreClass;
    }

    public String getCommand (String[] data, String dataline) {
        String command;
        if (data.length == 1) {
            command = dataline.trim();
            data[0] = command;
            dataline = null;
            return command;
        } else {
            command = data[0];
            data[data.length - 1] = data[data.length - 1].trim();
            dataline = null;
            return command;
        }
    }


    public void controllerRemove (String[] data, PrintWriter outController) {
        for (;;) {
            if (data.length != 2) {
                System.err.println("Malformed message received for Remove");
                continue;
            } // log error and continue
            String filename = data[1];
            File fileRemove = new File(subDstoreClass.getObject().getPath() + File.separator + filename);
            if (!fileRemove.exists() || !fileRemove.isFile()) {
                outController.println("ERROR_FILE_DOES_NOT_EXIST" + " " + filename);
            } else {
                fileRemove.delete();
                outController.println("REMOVE_ACK" + " " + filename);
            }
            break;
        }
    }


    public void controllerList(String[] data, PrintWriter outController) {
        for (;;) {
            if (data.length != 1) {
                System.err.println("Malformed message received for LIST");
                continue;
            } // log error and continue
            String[] fileList = subDstoreClass.getObject().getFolder().list();
            String listToSend = String.join(" ", fileList);
            outController.println("LIST" + " " + listToSend);
            break;
        }
    }

    public ArrayList<String> getInstructionList() {
        instructionList.add("REMOVE");
        instructionList.add("LIST");
        return instructionList;
    }

    public void controllerCommandHandler (String command, String[] data, PrintWriter outController) {
        //-----------------------------Controller Remove Command-----------------------------
        if (command.equals("REMOVE")) {
            controllerRemove(data, outController);
        //-----------------------------Controller List Command-----------------------------
        } else if (command.equals("LIST")) {
            controllerList(data, outController);

        }
    }

    @Override
    public void run() {
        try {
            BufferedReader inController = new BufferedReader(new InputStreamReader(controller.getInputStream()));
            PrintWriter outController = new PrintWriter(controller.getOutputStream(), true);
            String dataline = null;

            outController.println("JOIN" + " " + subDstoreClass.getObject().getPort());
            System.out.println("Entering loop of Controller");

            try {
                for (;;) {
                    dataline = inController.readLine();
                    if (dataline != null) {
                        String[] data = dataline.split(" ");

                        getCommand(data, dataline);
                        System.out.println("Revieved Controller Command: " + getCommand(data, dataline));

                        if (getInstructionList().contains(getCommand(data, dataline))) {
                            controllerCommandHandler(getCommand(data, dataline), data, outController);
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
