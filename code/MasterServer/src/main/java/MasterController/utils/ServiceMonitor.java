package MasterController.utils;

import MasterController.SocketController.SocketThread;
import MasterController.TableController;
import MasterController.TableController;
import MasterController.ZookeeperController;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.*;
import java.util.List;

/**
 * ZooKeeper的节点监视器，将发生的事件进行处理
 * 1. 从节点每次创表、插入、删除，在完成操作后将所修改的表和索引传输到ftp。
 * 2. 如果从节点挂了，主节点监测到后寻找表少的从节点，向该从节点发送备份指令（包括挂了的从节点所存储的所有表），从节点将表
 *    和索引从ftp上读取下来。读取完成后给主节点发消息，主节点收到后修改 table-server map
 * 3. 如果从节点重新连上，执行恢复策略，主节点向从节点发送恢复指令，"从节点收到后将本节点表全部删除"，删除完成后给主节点发消息，
 *    主节点收到后将其状态变更为有效的从节点，恢复正常使用
 */

@Slf4j
public class ServiceMonitor implements PathChildrenCacheListener {
    private CuratorController client;
    private ServiceStrategyExecutor strategyExecutor;
    private TableController tableController;

    public ServiceMonitor(CuratorController curatorClientController, TableController tableController) {
        this.tableController = tableController;
        this.strategyExecutor = new ServiceStrategyExecutor(tableController);
        this.client = curatorClientController;
    }

    @Override
    public void childEvent(CuratorFramework curatorFramework, PathChildrenCacheEvent event) throws Exception {

        String eventPath = event.getData() != null ? event.getData().getPath() : null;
        ChildData data = event.getData();
        // 接收到事件，对事件类型进行判断并执行相应策略
        switch (event.getType()) {
            case CHILD_ADDED:
                log.warn("服务器目录新增节点，Path = {}, Data = {}" , data.getPath(), new String(data.getData(), "UTF-8"));
                eventServerOnLine(
                        //去掉目录名，仅留下文件名，即主机名
                        eventPath.replaceFirst(ZookeeperController.ZK_PATH + "/", ""),
                        client.getNodeData(eventPath));
                break;
            case CHILD_UPDATED:
                log.warn("服务器目录更新节点，Path = {}, Data = {}" , data.getPath(), new String(data.getData(), "UTF-8"));
                eventServerUpdate(
                        eventPath.replaceFirst(ZookeeperController.ZK_PATH + "/", ""),
                        new String(event.getData().getData()));
                break;
            case CHILD_REMOVED:
                log.warn("服务器目录删除节点，Path = {}, Data = {}" , data.getPath(), new String(data.getData(), "UTF-8"));
                eventServerDisappear(
                        eventPath.replaceFirst(ZookeeperController.ZK_PATH + "/", ""),
                        client.getNodeData(eventPath));
                break;
            default:
        }
    }

    /**
     * 处理服务器节点上线事件，若服务器之前曾连接过，做恢复处理
     * 参数： 主机名：hostName，主机地址：hostUrl
     */
    public void eventServerOnLine(String hostName, String hostUrl) {
        log.warn("新增服务器节点：主机名 {}, 地址 {}", hostName, hostUrl);
        if (strategyExecutor.existServer(hostUrl)) {
            // 该服务器已经存在，即从失效状态中恢复
            log.warn("对该服务器{}执行恢复策略", hostName);
            strategyExecutor.execStrategy(hostUrl, ToleranceOperationType.RECOVER);
        }
        else {
            // 新发现的服务器，新增一份数据
            log.warn("对该服务器{}执行新增策略", hostName);
            strategyExecutor.execStrategy(hostUrl, ToleranceOperationType.DISCOVER);
        }
    }

    /**
     * 处理服务器节点失效事件
     * 参数： hostName hostUrl
     */
    public void eventServerDisappear(String hostName, String hostUrl) {
        log.warn("服务器节点失效：主机名 {}, 地址 {}", hostName, hostUrl);
        if (!strategyExecutor.existServer(hostUrl)) {
            throw new RuntimeException("需要删除信息的服务器不存在于服务器列表中");
        } else {
            // 更新并处理下线的服务器
            log.warn("对该服务器{}执行失效策略", hostName);
            strategyExecutor.execStrategy(hostUrl, ToleranceOperationType.INVALID);
        }
    }

    /**
     * 处理服务器节点更新事件
     *
     * 参数： hostName
     * 参数： hostUrl
     */
    public void eventServerUpdate(String hostName, String hostUrl) {
        log.warn("更新服务器节点：主机名 {}, 地址 {}", hostName, hostUrl);
        if (!strategyExecutor.existServer(hostUrl)) {
            throw new RuntimeException("需要更新信息的服务器不存在于服务器列表中");
        } else {
            // 更新服务器的URL ？？？
        }
    }
}

enum ToleranceOperationType {
    DISCOVER, RECOVER, INVALID;
}

@Slf4j
class ServiceStrategyExecutor {
    private TableController tableController;

    public ServiceStrategyExecutor(TableController tableController) {
        this.tableController = tableController;
    }

    public boolean existServer(String hostUrl) {
        return tableController.findServer(hostUrl);  // 查找该节点是否"连接过"
    }

    public void execStrategy(String hostUrl, ToleranceOperationType type) {
        try {
            switch (type) {
                case RECOVER:
                    execRecoverStrategy(hostUrl);
                    break;
                case DISCOVER:
                    execDiscoverStrategy(hostUrl);
                    break;
                case INVALID:
                    execInvalidStrategy(hostUrl);
                    break;
            }
        } catch (Exception e) {
            log.warn(e.getMessage(), e);
        }
    }

    private void execInvalidStrategy (String hostUrl) {
        StringBuffer allTable = new StringBuffer();
        List<String> tableList = tableController.getTableList(hostUrl);
        //格式为：M3@ip%name%name%name
        String bestInet = tableController.getBestServer(hostUrl);
        log.warn("bestInet:"+bestInet);
        allTable.append(hostUrl+"@");
        int i = 0;
        for(String s:tableList){
            if(i==0){
                allTable.append(s);
            }
            else {
                allTable.append("%");
                allTable.append(s);
            }
        }
        tableController.alterTable(bestInet,hostUrl);
        SocketThread socketThread = tableController.getSocket(bestInet);
        socketThread.sendMsg("M3"+allTable);
    }

    private void execDiscoverStrategy(String hostUrl) {
        int validServerNum = tableController.getValidServerNum();
        if (validServerNum < ZookeeperController.MIN_VALID_SERVER_NUM) {
            // 如果目前能够运行的服务器小于3台，提示无法完成服务
            log.warn("目前数据服务器数量少于{}，服务无法完成", ZookeeperController.MIN_VALID_SERVER_NUM);
            log.warn("目前数据服务器数量不足,服务处于非正常状态");
        }
    }

    private void execRecoverStrategy(String hostUrl) {
        tableController.recoverServer(hostUrl);
        SocketThread socketThread = tableController.getSocket(hostUrl);
        socketThread.sendMsg("M4");
        execDiscoverStrategy(hostUrl);
    }
}
