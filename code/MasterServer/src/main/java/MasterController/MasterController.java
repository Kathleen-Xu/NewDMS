package MasterController;

import MasterController.SocketController.SocketController;
import java.io.IOException;
public class MasterController {
    private ZookeeperController zookeeperController;
    private SocketController socketController;
    private TableController tableController;

    private final int PORT = 12345;

    public MasterController() throws IOException, InterruptedException {
        tableController = new TableController();
        zookeeperController = new ZookeeperController(tableController);
        socketController = new SocketController(PORT,tableController);
    }

    public void initialize() throws InterruptedException, IOException {


        Thread zkServiceThread = new Thread(zookeeperController);
        zkServiceThread.start();

        socketController.startService();
    }
}
