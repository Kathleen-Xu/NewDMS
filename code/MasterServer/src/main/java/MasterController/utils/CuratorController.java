package MasterController.utils;

import MasterController.ZookeeperController;
import org.apache.zookeeper.CreateMode;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.*;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class CuratorController {
    private CuratorFramework client = null;

    private ExecutorService ThreadPool = Executors.newFixedThreadPool(2);

    private String hostUrl = null;

    public CuratorController() {
        this.Connection(ZookeeperController.ZK_ADDRESS);
    }

    public CuratorController(String hostUrl) {
        this.Connection(hostUrl);
    }

    public CuratorFramework getClient() {
        return client;
    }

    public String getHostUrl() {
        return hostUrl;
    }

    /**
     * 在特定Host中连接ZooKeeper
     *
     * 参数： hostUrl
     */
    public void Connection(String hostUrl) {
        this.hostUrl = hostUrl;
        if (client == null) {
            // 多个客户端同时连接，创建有可能发生冲突，因此需要上锁
            synchronized (this) {
                client = CuratorFrameworkFactory.builder()
                        .connectString(hostUrl)
                        .connectionTimeoutMs(ZookeeperController.ZK_CONNECTION_TIMEOUT)
                        .sessionTimeoutMs(ZookeeperController.ZK_SESSION_TIMEOUT)
                        .retryPolicy(new ExponentialBackoffRetry(1000,3))   // 重试策略：初试时间为1s 重试10次
                        .build();

                // 开启连接
                client.start();
            }
        }
    }

//    /**
//     * 在特定路径上创建节点，并且设定一个特殊的值。默认为持久节点
//     *
//     * 参数： Path
//     * 参数： value
//     * 返回结果：
//     */
//    public String createNode(String Path, String value) throws Exception {
//        checkClientConnected();
//        /**
//         * 如果父节点不存在，会先创建父节点
//         */
//        return client.create().creatingParentsIfNeeded().forPath(Path, value.getBytes());
//    }

    /**
     * 创建节点
     *
     * 参数： Path 节点路径
     * 参数： value        值
     * 参数： nodeType     节点类型
     */
    public String createNode(String Path, String value, CreateMode nodeType) throws Exception {
        checkClientConnected();

        if (nodeType == null) {
            //throw new RuntimeException("未指定创建节点的类型");

            // 未输入创建类型。默认为持久节点
            return client.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath(Path, value.getBytes());
        } else if (checkNodeType(nodeType)) {
            return client.create().creatingParentsIfNeeded().withMode(nodeType).forPath(Path, value.getBytes());
        } else {
            throw new RuntimeException("创建节点类型不被采纳");
        }

//        if (nodeType == null) {
//            throw new RuntimeException("创建节点类型不合法");
//        } else if (CreateMode.PERSISTENT.equals(nodeType)) {
//            return client.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath(Path, value.getBytes());
//        } else if (CreateMode.EPHEMERAL.equals(nodeType)) {
//            return client.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).forPath(Path, value.getBytes());
//        } else if (CreateMode.PERSISTENT_SEQUENTIAL.equals(nodeType)) {
//            return client.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT_SEQUENTIAL).forPath(Path, value.getBytes());
//        } else if (CreateMode.EPHEMERAL_SEQUENTIAL.equals(nodeType)) {
//            return client.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL_SEQUENTIAL).forPath(Path, value.getBytes());
//        } else {
//            throw new RuntimeException("创建节点类型不被采纳");
//        }
    }

    /**
     * 判断节点类型是否为规定的四种类型中的一种
     *
     * 参数： 创建节点的类型：nodeType
     * 返回类型： 返回判断结果
     * */

    public Boolean checkNodeType(CreateMode nodeType) {
        return CreateMode.PERSISTENT.equals(nodeType) ||
                CreateMode.EPHEMERAL.equals(nodeType) ||
                CreateMode.PERSISTENT_SEQUENTIAL.equals(nodeType) ||
                CreateMode.EPHEMERAL_SEQUENTIAL.equals(nodeType);
    }

    /**
     * 获取节点的数据
     *
     * 参数： 节点路径：targetPath
     * 返回结果： 节点数据
     */
    public String getNodeData(String targetPath) throws Exception {
        checkClientConnected();
        return new String(client.getData().forPath(targetPath));
    }

    /**
     * 获取目录下的节点列表
     *
     * 参数： 目录路径：targetPath
     * 返回结果： 节点名字列表
     */
    public List<String> getDirChildren(String targetPath) throws Exception {
        checkClientConnected();
        return client.getChildren().forPath(targetPath);
    }

    /**
     * 检测节点是否存在
     *
     * 参数： targetPath
     * 返回结果：
     */
    public boolean checkNodeExist(String targetPath) throws Exception {
        checkClientConnected();
        return client.checkExists().forPath(targetPath) == null ? false : true;

    }

    /**
     * 监听数据节点的数据变化
     *
     * 参数： targetPath，listener
     * 异常抛出： Exception
     */
    public void monitorNode(String targetPath, NodeCacheListener listener) throws Exception {
        final NodeCache nodeCache = new NodeCache(client, targetPath, false);
        nodeCache.start(true);
        nodeCache.getListenable().addListener(new NodeCacheListener() {
            @Override
            public void nodeChanged() throws Exception {
                System.out.println("服务器节点【{}】数据发生变化，新数据为：{}"+targetPath+new String(nodeCache.getCurrentData().getData()));
            }
        }, ThreadPool);
    }

    /**
     * 监听子节点数据变化
     *
     * 参数： targetPath，listener
     * 异常抛出： Exception
     */
    public void monitorChildrenNodes(String targetPath, PathChildrenCacheListener listener) throws Exception {
        final PathChildrenCache childrenCache = new PathChildrenCache(client, targetPath, true);
        // 异步初始化cache，初始化完成触发完成事件
        childrenCache.start(PathChildrenCache.StartMode.POST_INITIALIZED_EVENT);
        // 在注册监听器的时候，如果传入此参数，当事件触发时，逻辑由线程池处理。如果不传会采用默认的线程池
        childrenCache.getListenable().addListener(listener, ThreadPool);
    }

    /**
     * 没有连接则进行连接
     *
     * */
    private void checkClientConnected() {
        if (client == null)
            this.Connection(ZookeeperController.ZK_ADDRESS);
    }
}
