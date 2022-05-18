package MasterController.SocketController;

import java.io.File;

public class BackupInfo {
    // 仅用于表示备份主机的一些变量

    public static final String BACKUP_HOST = "127.0.0.1";
    public static final int PORT = 3333;
    public static final String DEFAULT_DIR = "D:" + File.separatorChar + "backup";

    public static final String REGIONDATA_DIR = "D:" + File.separatorChar + "dbfiles";
}
