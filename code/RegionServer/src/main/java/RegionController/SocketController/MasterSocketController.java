package RegionController.SocketController;

import java.io.*;
import java.net.Socket;
import java.util.Hashtable;
import java.util.Map;

//import miniSQL.API;
import MasterController.SocketController.BackupInfo;
import com.sun.media.sound.DirectAudioDeviceProvider;
import miniSQL.*;
import miniSQL.CATALOGMANAGER.CatalogManager;
import miniSQL.CATALOGMANAGER.index;
import miniSQL.CATALOGMANAGER.table;
import miniSQL.Interpreter;

//import static RegionController.SocketController.ClientThread.StringToBufferedReader;

public class MasterSocketController implements Runnable {
    private Socket socket;
    private BufferedReader input = null;
    private PrintWriter output = null;
    private FileController fileController;
    //private DataBaseController dataBaseController;
    private boolean isRunning = false;

    public final int SERVER_PORT = 12345;
    public final String MASTER = "localhost";
    private Hashtable<String, table> tables;
    private Hashtable<String, index> indices;

    public MasterSocketController(FileController fileController) throws IOException {
        this.socket = new Socket(MASTER, SERVER_PORT);
//        this.ftpUtils = new FtpUtils();
        //this.dataBaseController = new DataBaseController();
        CatalogManager.InitialCatalog();
        this.tables = CatalogManager.getTables();
        this.indices = CatalogManager.getIndex();
        input = new BufferedReader(new InputStreamReader(socket.getInputStream()));
        output = new PrintWriter(socket.getOutputStream(), true);
        this.fileController = new FileController();
        isRunning = true;
    }

    public void sendToMaster(String modified_info) {
        output.println(modified_info);
    }

    public void sendTableInfoToMaster(String table_info) {
        output.println("R1" + table_info);
    }

    public String getMetaInfo() {
        StringBuilder result = new StringBuilder();
        for (Map.Entry<String, table> stringTableEntry : tables.entrySet()) {
            //System.out.println(((Map.Entry) stringTableEntry).getKey());
            result.append(((Map.Entry) stringTableEntry).getKey()).append(" ");
        }
        return result.toString();
    }
    public void receiveFromMaster() throws IOException {
        String line = null;
        if (socket.isClosed() || socket.isInputShutdown() || socket.isOutputShutdown()) {
            System.out.println("master_socket>>> closed");
        } else {
            line = input.readLine();
        }
        if (line != null) {
            if (line.startsWith("M3")) {
                // M3@ip%name%name%...
                fileController.Connect();
                String info = line.substring(2);
                if(line.length()==2) return;
                String[] tables = info.split("@")[1].split("%");
                //fileController.deleteLocalFile();
                fileController.GetFile(BackupInfo.REGIONDATA_DIR);
//                for(String table : tables) {
//                    fileController.downLoadFile("table", table, "");
//                    System.out.println("success " + table);
//                    ftpUtils.downLoadFile("index", table + "_index.index", "");
//                    System.out.println("success " + table + "_index.index");
//                }
//                String ip = info.split("#")[0];
//                ftpUtils.additionalDownloadFile("catalog", ip + "#table_catalog");
//                ftpUtils.additionalDownloadFile("catalog", ip + "#index_catalog");
                try {
                    API.Initialize();
                }
                catch (Exception e) {
                    e.printStackTrace();
                }finally {
                    fileController.Close();
                }
                System.out.println("here");
                output.println("R3 Complete disaster recovery");
            }
            else if (line.equals("M4")) {
                String tableName = getMetaInfo();
                String[] tableNames = tableName.split(" ");
                for(String table: tableNames) {
                    String tmp_sql = "drop table " + table + " ;";
                    //BufferedReader reader = StringToBufferedReader(tmp_sql);
                    InputStream is = new ByteArrayInputStream(tmp_sql.getBytes());
                    BufferedReader reader = new BufferedReader(new InputStreamReader(is));
                    Interpreter.Parsing(reader);
                    try {
                        API.store();
                        API.Initialize();
                    }
                    catch (Exception e) {
                        e.printStackTrace();
                    }
                }
                output.println("R4 ");
            }
        }
    }

    public void delFile(String fileName) {
        File file = new File(fileName);
        if (file.exists() && file.isFile()) file.delete();
    }

    @Override
    public void run() {
        System.out.println("master_socket>>> listening from master...");
        while (isRunning) {
            if (socket.isClosed() || socket.isInputShutdown() || socket.isOutputShutdown()) {
                isRunning = false;
                break;
            }
            try {
                receiveFromMaster();
            } catch (IOException e) {
                e.printStackTrace();
            }

            try {
                Thread.sleep(100);
            } catch (InterruptedException | NullPointerException e) {
                e.printStackTrace();
            }
        }
    }
}
