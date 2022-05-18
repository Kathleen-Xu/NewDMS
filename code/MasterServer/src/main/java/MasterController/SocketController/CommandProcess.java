package MasterController.SocketController;

import MasterController.TableController;
import MasterController.utils.SocketUtils;
//import javafx.scene.control.Tab;
import lombok.extern.slf4j.Slf4j;
import java.net.Socket;
import java.io.*;
@Slf4j
public class CommandProcess {
    private TableController tableController;
    private Socket socket;
    public CommandProcess(TableController tableController, Socket socket) {
        this.tableController = tableController;
        this.socket = socket;
    }
    //处理发送过来的命令，根据开头进行区分
    public String commandProcess(String cmd) {
        String result = "";
        log.warn(cmd);
        if (cmd.startsWith("C")) {
            String tablename = cmd.substring(2);
            if (cmd.startsWith("CN")) {
                result = "CN"+ tablename +" " + tableController.getipAddress(tablename) ;
            }
            else if (cmd.startsWith("CC")) {
                if (tableController.getipAddress((tablename)) == null)
                    result = "CC" + tablename+  " " +tableController.getBestServer() ;
                else
                    result = "CC" + tablename +" "+"Fail";
            }
            else {
                result = "wrong msg";
            }
            return result;
        }
        else if (cmd.startsWith("R")) {
            String ipAddress = socket.getInetAddress().getHostAddress();

            if (ipAddress.equals("127.0.0.1")) {
                ipAddress = SocketUtils.getHostAddress();
            }
            if (cmd.startsWith("R1") && !tableController.findServer(ipAddress)) {
                tableController.newServer(ipAddress);
                String[] AllTable = cmd.substring(2).split(" ");
                for (String s : AllTable) {
                    tableController.newTable(s, ipAddress);
                }
            }
            else if (cmd.startsWith("R2")) {
                String[] line = cmd.substring(2).split(" ");
                log.warn(line[0]);
                log.warn(line[1]);
                log.warn(ipAddress);
                if (line[1].equals("delete")) {
                    tableController.deleteTable((line[0]), ipAddress);
                }
                else if (line[1].equals("add")) {
                    tableController.newTable(line[0],ipAddress);
                }
            }
            else if (cmd.startsWith("R3")) {
                result = "migration succeed";
                log.warn("migration succeed");
            }
            else if (cmd.startsWith("R4")) {
                result = "recover success";
                log.warn("recover success");
            }
            else {
                result="wrong msg";
            }
            return result;

        }
        else {
            result = "wrong msg";
            return result;
        }
    }
}
