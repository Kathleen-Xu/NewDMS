import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

/**
 * Created with IntelliJ IDEA.
 *
 * @author : 徐凯琳
 * @version : 1.0
 * @Project : DMS
 * @Package : PACKAGE_NAME
 * @ClassName : ClientProcessor.java
 * @createTime : 2022/5/13 16:57
 * @Email : 2628968409@qq.com
 * @Description :
 */
public class ClientProcessor {
    // master
    private final String ipOfMaster = "localhost";
    private final int portOfMaster = 12345;
    private Socket socketOfMaster;
    private BufferedReader inputOfMaster;
    private PrintWriter outputOfMaster;
    private boolean isRunningOfMaster;
    private Thread listenerOfMaster;
    // region
    private final int portOfRegion = 22222;
    private Socket socketOfRegion;
    private BufferedReader inputOfRegion;
    private PrintWriter outputOfRegion;
    private boolean isRunningOfRegion;
    private Thread listenerOfRegion;
    // SQL-command
    private Map<String, String> cmdMap = new HashMap<>();
    // cache
    private Map<String, String> cache = new HashMap<>();

    public ClientProcessor() throws IOException {
        // connectToMaster
        System.out.println("INFO: Start connecting to master server...");
        socketOfMaster = new Socket(ipOfMaster, portOfMaster);
        inputOfMaster = new BufferedReader(new InputStreamReader(socketOfMaster.getInputStream()));
        outputOfMaster = new PrintWriter(socketOfMaster.getOutputStream(), true);
        isRunningOfMaster = true;
        System.out.println("INFO: Connect to master server successfully!");
        listenerOfMaster = new ListenerOfMaster();
        listenerOfMaster.start();

    }

    public boolean connectToRegion(String ip)  {
        System.out.println("INFO: Start connecting to region server...");
        boolean isConnected = false;
        try {
            socketOfRegion = new Socket(ip, portOfRegion);
            inputOfRegion = new BufferedReader(new InputStreamReader(socketOfRegion.getInputStream()));
            outputOfRegion = new PrintWriter(socketOfRegion.getOutputStream(), true);
            isRunningOfRegion = true;
            System.out.println("INFO: Connect to region server successfully!");
            listenerOfRegion = new ListenerOfRegion();
            listenerOfRegion.start();
            isConnected = true;
        } catch (Exception ignored) {

        }
        return isConnected;
    }

    public void run() throws IOException, InterruptedException {
        System.out.println("INFO: Client starts running...");
        Scanner input = new Scanner(System.in);
        String line, lines;
        int len;
        System.out.println("INFO: Please input SQL command...");
        while (true) {
            lines = line = "";
            line = input.nextLine().trim();
            len = line.length();
            while (len == 0 || line.charAt(len - 1) != ';') {
                if (len != 0) {
                    lines = lines + line + " ";
                }
                line = input.nextLine().trim();
                len = line.length();
            }
            lines = lines + line;
            lines = lines.trim();
            // 遇到"quit;"指令后直接退出
            if (lines.equals("quit;")) {
                closeSocket(socketOfMaster);
                // 关闭regionSocket
                break;
            }
            Map<String, String> result = interpreter(lines);
            // 根据解析得到的result做对应的处理
            String tableName = result.get("name");
            String type = result.get("type");
            cmdMap.put(tableName, lines); // 将表名&命令存放如map中
            if (type.equals("create")) {
                outputOfMaster.println("CC" + tableName);
            } else if (!cache.containsKey(tableName)) {
                outputOfMaster.println("CN" + tableName);
            } else {
                System.out.println("INFO: Find the table in cache.");
                if (connectToRegion(cache.get(tableName))) {
                    Thread.sleep(100);
                    outputOfRegion.println("CI" + lines);
                } else {
                    System.out.println("INFO: Cannot connect to region server! Cache expired!");
                    System.out.println("INFO: Cache updating...");
                    outputOfMaster.println("CN" + tableName);
                }
            }
            Thread.sleep(200);
        }
    }

    public Map<String, String> interpreter(String cmd) {
        Map<String, String> res = new HashMap<>();
        // cache
        String[] words = cmd.replaceAll("\\s+", " ").toLowerCase().split(" ");
        String type = words[0];
        res.put("type", type);
        switch (type) {
            case "create":
                //res.put("name", words[2]); break;
            case "drop":
            case "insert":
            case "delete":
                res.put("name", words[2].replaceAll("[;()]", "")); break;
            case "select":
                for (int i = 1; i < words.length - 1; i++) {
                    if (words[i].equals("from")) {
                        res.put("name", words[i + 1].replaceAll("[;()]", ""));
                    }
                } break;
            default:
        }
        if (res.containsKey("name")) {
            res.put("state", "normal");
            System.out.println("type: " + res.get("type"));
            System.out.println("tableName: " + res.get("name"));
        } else {
            res.put("state", "error");
        }
        return res;

    }

    public void receiveFromMaster() throws IOException, InterruptedException {
        if (socketOfMaster.isClosed() || socketOfMaster.isInputShutdown() || socketOfMaster.isOutputShutdown()) {
            System.out.println("INFO: Socket of master has closed!");
        } else {
            String msg = inputOfMaster.readLine();
            System.out.println("INFO: Message received from master server is:\n" + msg);
            if (msg.charAt(0) == 'M' && (msg.charAt(1) == 'N' || msg.charAt(1) == 'C')) {
                String name = msg.substring(2).split(" ")[0];
                String ip = msg.substring(2).split(" ")[1];
                if (!ip.equals("Fail")) {
                    // 将tableName与所存储的从服务器的ip地址存入cache
                    cache.put(name, ip);
                    System.out.println("INFO: Cache saved. [table]" + name + ", [ip]" + ip);
                    // 用ip连接从服务器，通过tableName从Map得到sql命令。
                    String cmd = cmdMap.get(name);
                    if (connectToRegion(ip)) {
                        Thread.sleep(100);
                        outputOfRegion.println("CI" + cmd);
                    } else {
                        System.out.println("Error: CANNOT connect to region server!");
                        System.out.println("INFO: PLEASE check the network and input SQL command again.");
                    }
                } else {
                    System.out.println("This table is already existed.");
                }

            }
        }
    }

    public void receiveFromRegion() throws IOException {
        if (socketOfRegion.isClosed() || socketOfRegion.isInputShutdown() || socketOfRegion.isOutputShutdown()) {
            System.out.println("INFO: Socket of region has closed!");
        } else {
            String msg = inputOfRegion.readLine();
            System.out.println("INFO: Message received from region server is:\n" + msg);
        }
    }

    public void closeSocket(Socket socket) throws IOException {
        socket.shutdownInput();
        socket.shutdownOutput();
        socket.close();
    }

    class ListenerOfMaster extends Thread {
        @Override
        public void run() {
            System.out.println("INFO: Start listening to master server...");
            while (isRunningOfMaster) {
                if (socketOfMaster.isClosed() || socketOfMaster.isInputShutdown() || socketOfMaster.isOutputShutdown()) {
                    isRunningOfMaster = false;
                    break;
                }
                try {
                    receiveFromMaster();
                } catch (IOException | InterruptedException e) {
                    System.out.println("INFO: Disconnected from master server!");
                    System.out.println("INFO: Please input SQL command...");
                    break;
                }
            }
        }
    }

    class ListenerOfRegion extends Thread {
        @Override
        public void run() {
            System.out.println("INFO: Start listening to region server...");
            while (isRunningOfRegion) {
                if (socketOfRegion.isClosed() || socketOfRegion.isInputShutdown() || socketOfRegion.isOutputShutdown()) {
                    isRunningOfRegion = false;
                    break;
                }
                try {
                    receiveFromRegion();
                    System.out.println("INFO: Please input SQL command...");
                } catch (IOException e) {
                    System.out.println("INFO: Disconnected from region server!");
                    System.out.println("INFO: Please input SQL command...");
                    break;
                }
            }
        }
    }
}
