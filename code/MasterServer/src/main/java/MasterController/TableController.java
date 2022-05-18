package MasterController;

import MasterController.SocketController.SocketThread;
import java.io.*;
import java.util.*;
import lombok.extern.slf4j.Slf4j;
@Slf4j
public class TableController {
    //记录tablename和ip的键值对表
    private Map<String, String> InfoTable;
    //所有连接过的从节点的ip
    private List<String> AllServer;
    //在线的从节点的ip及其下所存储的数据表的键值对表
    private  Map<String, List<String>> OnlineServer;
    //和不同从节点的socket连接
    private Map<String, SocketThread> Socketmap;

    //初始化
    public TableController() throws IOException {
        InfoTable = new HashMap<>();
        AllServer = new ArrayList<>();
        OnlineServer = new HashMap<>();
        Socketmap = new HashMap<>();
    }
    //新增数据表
    public void newTable(String tablename, String ipAddress) {
        InfoTable.put(tablename, ipAddress);
        if (OnlineServer.containsKey(ipAddress)) {
            OnlineServer.get(ipAddress).add(tablename);
        }
        else {
            List<String> temp = new ArrayList<>();
            temp.add(tablename);
            OnlineServer.put(ipAddress, temp);
        }
    }
    //删除数据表
    public void deleteTable(String tablename, String ipAddress) {
        InfoTable.remove(tablename);
        OnlineServer.get(ipAddress).removeIf((tablename::equals));
    }
    //获得当前在线从节点的个数
    public int getValidServerNum() {
        return OnlineServer.size();
    }
    //获取指定数据表存在的从节点的ip地址
    public String getipAddress(String tablename) {
        for (Map.Entry<String, String> entry : InfoTable.entrySet()) {
            if (entry.getKey().equals(tablename)) {
                return entry.getValue();
            }
        }
        for (Map.Entry<String, String> entry : InfoTable.entrySet()) {
            log.warn(entry.getKey());
            log.warn(entry.getValue());
        }
        return null;
    }
    //负载均衡，获得当前最合适的服务器
    public String getBestServer() {
        int min = Integer.MAX_VALUE;
        String result = "";
        for (Map.Entry<String, List<String>> entry : OnlineServer.entrySet()) {
            if (entry.getValue().size() < min) {
                min = entry.getValue().size();
                result = entry.getKey();
            }
        }
        return result;
    }
    //负载均衡，在不选择指定host的情况下获取最合适的服务器
    public String getBestServer(String host) {
        int min = Integer.MAX_VALUE;
        String result= "";
        for (Map.Entry<String, List<String>> entry : OnlineServer.entrySet()) {
            if (!entry.getKey().equals(host) && entry.getValue().size() < min) {
                min = entry.getValue().size();
                result = entry.getKey();
            }
        }
        return result;
    }
    //查找指定从节点是否存在
    public boolean findServer(String host) {
        for (String s : AllServer) {
            if (s.equals(host)) {
                return true;
            }
        }
        return false;
    }
    //注册从节点
    public void newServer(String host) {
        if (!findServer(host)) {
            AllServer.add(host);
        }
        List<String> temp = new ArrayList<>();
        OnlineServer.put(host, temp);
    }
    //获取指定从节点存储的数据表列表
    public List<String> getTableList(String host) {
        for (Map.Entry<String, List<String>> entry : OnlineServer.entrySet()) {
            if (entry.getKey().equals(host)) {
                return entry.getValue();
            }
        }
        return null;
    }

    //保存Socket连接
    public void newSocket(String host, SocketThread socketThread) {
        Socketmap.put(host, socketThread);
    }
    //获取指定从节点的socket连接
    public SocketThread getSocket(String host) {
        for (Map.Entry<String, SocketThread> entry : Socketmap.entrySet()) {
            if (entry.getKey().equals(host)) {
                return entry.getValue();
            }
        }
        return null;
    }
    //容错容灾迁移数据表
    public void alterTable(String ipAddress, String host) {
        List <String> tableList = getTableList(host);
        for (String table :tableList) {
            InfoTable.put(table, ipAddress);
        }
        List <String> targetipTable = OnlineServer.get(ipAddress);
        targetipTable.addAll(tableList);
        OnlineServer.put(ipAddress, targetipTable);
        OnlineServer.remove(host);
    }
    //从节点恢复上线
    public void recoverServer(String host) {
        List<String> temp = new ArrayList<>();
        OnlineServer.put(host, temp);
    }
}
