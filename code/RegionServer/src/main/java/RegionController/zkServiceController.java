package RegionController;

import MasterController.ZookeeperController;
import MasterController.utils.CuratorController;
import MasterController.utils.SocketUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.zookeeper.CreateMode;

/**
 *  @description: ZooKeeper 服务管理逻辑
 */
@Slf4j
public class zkServiceController implements Runnable {
    @Override
    public void run() {
        this.serviceRegister();
    }

    private void serviceRegister() {
        try {
            // 向ZooKeeper注册临时节点
            CuratorController curatorClientController = new CuratorController();
            int nChildren = curatorClientController.getDirChildren(ZookeeperController.ZK_PATH).size();
            if(nChildren==0)
                curatorClientController.createNode(getRegisterPath() + nChildren, SocketUtils.getHostAddress(), CreateMode.EPHEMERAL);
            else{
                //获取所有节点中最后一个节点的名字的编号，在其基础上加1,“7”是因为节点的固定前缀Region_
                String index = String.valueOf(Integer.parseInt((curatorClientController.getDirChildren(ZookeeperController.ZK_PATH)).get(nChildren - 1).substring(7)) + 1);
                curatorClientController.createNode(getRegisterPath() + index, SocketUtils.getHostAddress(), CreateMode.EPHEMERAL);
            }

            // 阻塞该线程，直到发生异常或者主动退出
            synchronized (this) {
                wait();
            }
        } catch (Exception e) {
            log.warn(e.getMessage(), e);
        }
    }

    /**
     * @description: 获取Zookeeper注册的路径
     */
    private static String getRegisterPath() {
        return ZookeeperController.ZK_PATH + "/" + ZookeeperController.HOST_NAME_PREFIX;
    }
}
