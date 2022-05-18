package RegionController.SocketController;

import java.io.*;
import java.util.Scanner;
import java.util.stream.Collectors;

import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.commons.net.ftp.FTPReply;

public class FtpUtils {
    // 此处设置为FTP的IP地址
    public String hostname = "192.168.179.1";
    public int port = 21;
    public String username = "test";
    public String password = "test";
    private static final int BUFFER_SIZE = 1024 * 1024 * 4;
    public FTPClient ftpClient = null;

    private void login() {
        ftpClient = new FTPClient();
        ftpClient.setControlEncoding("utf-8");
        try {
            ftpClient.enterLocalPassiveMode();
            ftpClient.connect(hostname, port);
            ftpClient.login(username, password);
            ftpClient.setFileType(FTPClient.BINARY_FILE_TYPE);
            ftpClient.setBufferSize(BUFFER_SIZE);
            int reply = ftpClient.getReplyCode();
            if (!FTPReply.isPositiveCompletion(reply)) {
                closeConnect();
                System.out.println("FTP>>> fail to connect...");
            }
            else{
                System.out.println("FTP>>> connect successfully!");
            }
            System.out.println("FTP>>> login successfully!");
        } catch (Exception e) {
            System.out.println("FTP>>> fail to login..." + e.getMessage());
        }
    }

    private void closeConnect() {
        if (ftpClient != null && ftpClient.isConnected()) {
            try {
                ftpClient.logout();
                ftpClient.disconnect();
                System.out.println("FTP>>> close successfully!");
            } catch (IOException e) {
                System.out.println("FTP>>> fail to close..." + e.getMessage());
            }
        }
    }

    public Boolean downLoadFile(String ftpPath, String fileName, String savePath) {
        login();
        OutputStream os = null;
        if (ftpClient != null) {
            try {
                if (!ftpClient.changeWorkingDirectory(ftpPath)) {
                    System.out.println("/" + ftpPath + "directory nonexistent");
                    return false;
                }
                ftpClient.enterLocalPassiveMode();

                FTPFile[] ftpFiles = ftpClient.listFiles();

                if (ftpFiles == null || ftpFiles.length == 0) {
                    System.out.println("/" + ftpPath + "no file in this directory");
                    return false;
                }
                for(FTPFile file : ftpFiles){
                    if(fileName.equals("") || fileName.equalsIgnoreCase(file.getName())) {
                        if(!file.isDirectory()) {
                            File saveFile = new File(savePath + file.getName());
                            os = new FileOutputStream(saveFile);
                            ftpClient.retrieveFile(file.getName(), os);
                            os.close();
                        }
                    }
                }
                return true;
            } catch (IOException e) {
                System.out.println("fail to download" + e.getMessage());
            } finally {
                if(null != os) {
                    try {
                        os.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
                closeConnect();
            }
        }
        return false;
    }

    public boolean additionalDownloadFile(String ftpPath, String fileName) {
        login();
        OutputStream os = null;
        if (ftpClient != null) {
            try {
                if (!ftpClient.changeWorkingDirectory(ftpPath)) {
                    System.out.println("/" + ftpPath + "directory nonexistent");
                    return false;
                }
                ftpClient.enterLocalPassiveMode();

                FTPFile[] ftpFiles = ftpClient.listFiles();

                if (ftpFiles == null || ftpFiles.length == 0) {
                    System.out.println("/" + ftpPath + "no file in this directory");
                    return false;
                }
                for(FTPFile file : ftpFiles){
                    if(fileName.equals("") || fileName.equalsIgnoreCase(file.getName())) {
                        if(!file.isDirectory()) {
                            File saveFile = new File(file.getName().split("#")[1]);
                            os = new FileOutputStream(saveFile, true);
                            ftpClient.retrieveFile(file.getName(), os);
                            os.close();
                        }
                    }
                }
                return true;
            } catch (IOException e) {
                System.out.println("fail to download" + e.getMessage());
            } finally {
                if(null != os) {
                    try {
                        os.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
                closeConnect();
            }
        }
        return false;
    }

    public boolean uploadFile(String fileName, String savePath) {

        login();
        boolean flag = false;
        InputStream inputStream = null;
        boolean judge = false;
        if (ftpClient != null) {
            try{
                //System.out.println("用户的当前工作目录:/n"+System.getProperty("user.dir"));
                inputStream = new FileInputStream(new File(fileName));
                String tmp = new BufferedReader(new InputStreamReader(inputStream))
                        .lines().collect(Collectors.joining(System.lineSeparator()));
//                String tmp = inputStream.toString();
                System.out.println("inputstream--"+inputStream+"   "+tmp);
                ftpClient.setFileType(FTPClient.BINARY_FILE_TYPE);
                ftpClient.makeDirectory(savePath);
                ftpClient.changeWorkingDirectory(savePath);
                judge = ftpClient.storeFile(fileName, inputStream);
                inputStream.close();
                flag = true;
                if (judge==true){
                    System.out.println("upload "+fileName+" to "+savePath+" successfully!");
                }

            } catch (Exception e) {
                System.out.println("fail to upload "+fileName);
                e.printStackTrace();
            } finally {
                if(null != inputStream) {
                    try {
                        inputStream.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
                closeConnect();
            }
        }
        return flag;
    }

    public boolean uploadFile(String fileName, String IP, String savePath) {
        login();
        boolean flag = false;
        InputStream inputStream = null;
        boolean judge = false;
        if (ftpClient != null) {
            try{
                System.out.println("ip_inputstream--"+inputStream);
                inputStream = new FileInputStream(new File(fileName));
                ftpClient.setFileType(FTPClient.BINARY_FILE_TYPE);
                ftpClient.makeDirectory(savePath);
                ftpClient.changeWorkingDirectory(savePath);
                judge = ftpClient.storeFile(fileName, inputStream);
                ftpClient.rename(fileName, "/catalog/" + IP + "#" + fileName);
                inputStream.close();
                flag = true;
                if (judge==true){
                    System.out.println("ip_upload "+fileName+" to "+savePath+" successfully!");
                }

            } catch (Exception e) {
                e.printStackTrace();
                System.out.println("ip_fail to upload "+fileName);

            } finally {
                if(null != inputStream) {
                    try {
                        inputStream.close();

                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
                closeConnect();
            }
        }
        return flag;
    }

    public boolean deleteFile(String fileName, String filePath) {
        login();
        boolean flag = false;
        if (ftpClient != null) {
            try {
                ftpClient.changeWorkingDirectory(filePath);
                ftpClient.dele(fileName);
                flag = true;
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                closeConnect();
            }
        }
        return flag;
    }
}
