package RegionController;

import RegionController.SocketController.ClientSocketController;
import RegionController.SocketController.FileController;
import RegionController.SocketController.MasterSocketController;
//import miniSQL.API;
import miniSQL.*;
import miniSQL.CATALOGMANAGER.CatalogManager;
import miniSQL.CATALOGMANAGER.index;
import miniSQL.CATALOGMANAGER.table;

import java.io.IOException;
import java.util.Hashtable;
import java.util.Map;

public class RegionController {
    //private DataBaseController dataBaseController;
    private ClientSocketController clientSocketController;
    private MasterSocketController masterSocketController;
    private zkServiceController _zkServiceController;

    private FileController fileController;

    private final int PORT = 22222;

    private Hashtable<String, table> tables;
    private Hashtable<String, index> indices;


    public RegionController() throws IOException, InterruptedException {
        CatalogManager.InitialCatalog();
        this.tables = CatalogManager.getTables();
        this.indices = CatalogManager.getIndex();
        fileController = new FileController();
        //dataBaseController = new DataBaseController();
        _zkServiceController = new zkServiceController();
        masterSocketController = new MasterSocketController(fileController);
        //masterSocketController.sendTableInfoToMaster(dataBaseController.getMetaInfo());
        masterSocketController.sendTableInfoToMaster(getMetaInfo());
        clientSocketController = new ClientSocketController(PORT, masterSocketController, fileController);
        Thread centerThread = new Thread(clientSocketController);
        centerThread.start();
    }

    public String getMetaInfo() {
        StringBuilder result = new StringBuilder();
        for (Map.Entry<String, table> stringTableEntry : tables.entrySet()) {
            //System.out.println(((Map.Entry) stringTableEntry).getKey());
            result.append(((Map.Entry) stringTableEntry).getKey()).append(" ");
        }
        return result.toString();
    }
    public void run() throws Exception {
        // 线程1：在应用启动的时候自动将本机的Host信息注册到ZooKeeper，然后阻塞，直到应用退出的时候也同时退出

        API.Initialize();
        Thread zkServiceThread = new Thread(_zkServiceController);
        zkServiceThread.start();
        Thread MasterSocketThread = new Thread(masterSocketController);
        MasterSocketThread.start();

        System.out.println("start running");
    }
}


