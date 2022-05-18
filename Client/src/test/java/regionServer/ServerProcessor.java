package regionServer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;

/**
 * Created with IntelliJ IDEA.
 *
 * @author : 徐凯琳
 * @version : 1.0
 * @Project : DMS
 * @Package : PACKAGE_NAME
 * @ClassName : regionServer.ServerProcessor.java
 * @createTime : 2022/5/13 18:26
 * @Email : 2628968409@qq.com
 * @Description :
 */
public class ServerProcessor {
    private ServerSocket serverSocket;
    public ServerProcessor() throws IOException {
        serverSocket = new ServerSocket(22222);
    }
    public void run() throws IOException {
        System.out.println("INFO: Region server has started successfully...");
        while (true) {
            Socket socket= serverSocket.accept();
            new Thread(new Listener(socket)).start();
        }
    }

}

class Listener extends Thread {
    private final Socket socket;
    public Listener (Socket socket) {
        this.socket = socket;
    }

    @Override
    public void run() {
        try {
            BufferedReader br = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            PrintWriter pw = new PrintWriter(socket.getOutputStream(), true);
            while (true) {
                String msg = br.readLine();
                System.out.println("INFO: Message received from client is:\n" + msg);
                pw.println("Over!");
            }
            //socket.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
