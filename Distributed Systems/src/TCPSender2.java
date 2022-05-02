import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.Socket;

public class TCPSender2 {
    public static void main(String[] args) {
        Socket socket = null;
        try {
            InetAddress address = InetAddress.getLocalHost();
            socket = new Socket(address, 4322);
            PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
            for (int i = 0; i < 10; i ++) {
                out.println("TCPSender2 Message" + i);
                Thread.sleep(1000);
            }
        } catch (Exception e) {
            System.err.println("error e: " + e);
        } finally {
            if (socket != null) {
                try {
                    socket.close();
                } catch (IOException e) {
                    System.err.println("error: " + e);
                }
            }
        }
    }
}
