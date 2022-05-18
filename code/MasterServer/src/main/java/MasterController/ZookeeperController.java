package MasterController;

import MasterController.utils.CuratorController;
import MasterController.utils.ServiceMonitor;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.api.CreateModable;
import org.apache.zookeeper.CreateMode;

@Slf4j
public class ZookeeperController implements Runnable {
    private TableController tableController;

    public static final String ZK_ADDRESS = "localhost:2181";

    public static final Integer ZK_SESSION_TIMEOUT = 3000;
    public static final Integer ZK_CONNECTION_TIMEOUT = 3000;

    public static final String ZK_PATH = "/db";

    public static final String HOST_NAME_PREFIX = "Region_";

    public static final int MIN_VALID_SERVER_NUM = 3;

    public ZookeeperController(TableController tableController) {
        this.tableController = tableController;
    }

    @Override
    public void run() {
        this.startMonitor();
    }

    // 一些zookeeper的测试代码
    public void startMonitor(){
        try {
            // 开启一个连接
            CuratorController curatorClientController = new CuratorController(ZK_ADDRESS);
            // 创建服务器主目录
            if (!curatorClientController.checkNodeExist(ZK_PATH)){
                curatorClientController.createNode(ZK_PATH, "服务器主目录", CreateMode.PERSISTENT);
            }

            // 开始监听服务器目录，如果有节点的变化，则处理相应事件
            curatorClientController.monitorChildrenNodes(ZK_PATH, new ServiceMonitor(curatorClientController, tableController));
        } catch (Exception e) {
            log.warn(e.getMessage(),e);
        }
    }
}
