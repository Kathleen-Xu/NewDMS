package MasterController;

import MasterController.SocketController.SocketThread;
import java.io.*;
import java.util.*;
import lombok.extern.slf4j.Slf4j;
@Slf4j
public class TableController {
    //��¼tablename��ip�ļ�ֵ�Ա�
    private Map<String, String> InfoTable;
    //�������ӹ��Ĵӽڵ��ip
    private List<String> AllServer;
    //���ߵĴӽڵ��ip���������洢�����ݱ�ļ�ֵ�Ա�
    private  Map<String, List<String>> OnlineServer;
    //�Ͳ�ͬ�ӽڵ��socket����
    private Map<String, SocketThread> Socketmap;

    //��ʼ��
    public TableController() throws IOException {
        InfoTable = new HashMap<>();
        AllServer = new ArrayList<>();
        OnlineServer = new HashMap<>();
        Socketmap = new HashMap<>();
    }
    //�������ݱ�
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
    //ɾ�����ݱ�
    public void deleteTable(String tablename, String ipAddress) {
        InfoTable.remove(tablename);
        OnlineServer.get(ipAddress).removeIf((tablename::equals));
    }
    //��õ�ǰ���ߴӽڵ�ĸ���
    public int getValidServerNum() {
        return OnlineServer.size();
    }
    //��ȡָ�����ݱ���ڵĴӽڵ��ip��ַ
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
    //���ؾ��⣬��õ�ǰ����ʵķ�����
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
    //���ؾ��⣬�ڲ�ѡ��ָ��host������»�ȡ����ʵķ�����
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
    //����ָ���ӽڵ��Ƿ����
    public boolean findServer(String host) {
        for (String s : AllServer) {
            if (s.equals(host)) {
                return true;
            }
        }
        return false;
    }
    //ע��ӽڵ�
    public void newServer(String host) {
        if (!findServer(host)) {
            AllServer.add(host);
        }
        List<String> temp = new ArrayList<>();
        OnlineServer.put(host, temp);
    }
    //��ȡָ���ӽڵ�洢�����ݱ��б�
    public List<String> getTableList(String host) {
        for (Map.Entry<String, List<String>> entry : OnlineServer.entrySet()) {
            if (entry.getKey().equals(host)) {
                return entry.getValue();
            }
        }
        return null;
    }

    //����Socket����
    public void newSocket(String host, SocketThread socketThread) {
        Socketmap.put(host, socketThread);
    }
    //��ȡָ���ӽڵ��socket����
    public SocketThread getSocket(String host) {
        for (Map.Entry<String, SocketThread> entry : Socketmap.entrySet()) {
            if (entry.getKey().equals(host)) {
                return entry.getValue();
            }
        }
        return null;
    }
    //�ݴ�����Ǩ�����ݱ�
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
    //�ӽڵ�ָ�����
    public void recoverServer(String host) {
        List<String> temp = new ArrayList<>();
        OnlineServer.put(host, temp);
    }
}
