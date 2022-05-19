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

    public static void main(String[] args) throws IOException {
        int port = Integer.parseInt(args[0]);
        int cport = Integer.parseInt(args[1]);
        int timeout = Integer.parseInt(args[2]);
        String file_folder = args[3];
        Dstore dstore = new Dstore(port, cport, timeout, file_folder);
    }
}