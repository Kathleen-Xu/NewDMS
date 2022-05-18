package MasterController.SocketController;

import MasterController.TableController;
import MasterController.utils.SocketUtils;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

public class SocketController {
    private ServerSocket serverSocket;
    private TableController tableController;

    public SocketController(int port,TableController tableController)
            throws IOException, InterruptedException {
        this.tableController = tableController;
        this.serverSocket = new ServerSocket(port);
    }
    //等待与Master连接的socket
    public void startService() throws InterruptedException, IOException {
        while (true) {
            Thread.sleep(200);
            Socket socket = serverSocket.accept();
            SocketThread socketThread = new SocketThread(socket,tableController);
            String ipAddress = socket.getInetAddress().getHostAddress();
            if(ipAddress.equals("127.0.0.1"))
                ipAddress = SocketUtils.getHostAddress();
            tableController.newSocket(ipAddress,socketThread);
            Thread thread = new Thread(socketThread);
            thread.start();
        }
    }
}
