import MasterController.MasterController;
import org.apache.zookeeper.KeeperException;
import java.io.IOException;

public class MasterServer {
    public static void main(String[] args) throws IOException, KeeperException, InterruptedException {
        MasterController masterController = new MasterController();
        masterController.initialize();
    }
}
