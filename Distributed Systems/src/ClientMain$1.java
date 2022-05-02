import java.io.File;

class ClientMain$1 extends Thread {
    ClientMain$1(int var1, int var2, File var3, File var4) {
        this.val$cport = var1;
        this.val$timeout = var2;
        this.val$downloadFolder = var3;
        this.val$uploadFolder = var4;
    }

    public void run() {
        ClientMain.test2Client(this.val$cport, this.val$timeout, this.val$downloadFolder, this.val$uploadFolder);
    }
}
