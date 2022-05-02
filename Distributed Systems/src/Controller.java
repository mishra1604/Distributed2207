import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;

public class Controller {

    int cport;
    int R;
    int timeout; // milliseconds
    int rebalance_period;

    public Controller (int cport, int R, int timeout, int rebalance_period){
        this.cport = cport;
        this.R = R;
        this.timeout = timeout;
        this.rebalance_period = rebalance_period;
    }

    public void storeFile(String operation, String filename, String fileSize){

    }

    public void loadFile(String operation, String filename) {

    }

    public void removeFile(String operation, String filename) {

    }

    public void listOperation(String operation, String[] filenames){

    }




    public static void main(String[] args) {

        Controller controller = new Controller(Integer.parseInt(args[0]),Integer.parseInt(args[1]),Integer.parseInt(args[2]),Integer.parseInt(args[3]));

        ServerSocket ss = null;
        try {
            ss = new ServerSocket(4322);
            while (true) {
                Socket client = ss.accept();
                new Thread(new ServiceThread(client)).start();
            }
        } catch (Exception e) {
            System.err.println("error line 17: " + e);
        } finally {
            if (ss != null) {
                try {
                    ss.close();
                } catch (IOException e) {
                    System.err.println("error line 23: " + e);
                }
            }
        }
    }
    static class ServiceThread implements Runnable {
        Socket client;
        ServiceThread(Socket c) {
            client = c;
        }
        public void run(){
            try {
                BufferedReader in = new BufferedReader(new InputStreamReader(client.getInputStream()));
                String line;
                while ((line = in.readLine()) != null) {
                    System.out.println(line + "received");
                }
                client.close();
            } catch (Exception e) {
                System.err.println("error line 35: " + e);
            }
        }
    }
}
