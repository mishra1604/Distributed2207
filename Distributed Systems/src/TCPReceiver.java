import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;

public class TCPReceiver {
    public static void main(String[] args) {
        ServerSocket ss = null; // serversocket used for listening to incoming TCP connections
        try {
            ss = new ServerSocket(4322); //bound to a specific port
            while(true) {
                try{
                    Socket client = ss.accept(); // used as an endpoint of a TCP Connection
                    BufferedReader in = new BufferedReader(new InputStreamReader(client.getInputStream()));
                    String line;
                    while((line = in.readLine()) != null) System.out.println(line + " received");
                    client.close();
                } catch (Exception e) {
                    System.err.println("error: " + e);
                }
            }
        } catch (Exception e) {
            System.err.println("error: " + e);
        } finally {
            if (ss != null) {
                try {
                    ss.close();
                } catch (IOException e) {
                    System.err.println("error: " + e);
                }
            }
        }
    }
}
