package RegionController.SocketController;

import MasterController.SocketController.BackupInfo;

import java.net.*;
import java.io.*;

public class FileController {
    private Socket socket;
    private BufferedReader in;
    private PrintWriter out;
    private FileInputStream file_in;
    private FileOutputStream file_out;
    DataOutputStream dos;             //
    DataInputStream dis;              //
    private boolean running = true;

    public FileController() {

    }

    public boolean IsRunning() {
        return running;
    }

    public void Connect() {
        try {
            socket = new Socket(BackupInfo.BACKUP_HOST, BackupInfo.PORT);
        } catch(IOException e) {
            // If the creation of the socket fails,
            // nothing needs to be cleaned up.
        }
        try {
            in = new BufferedReader(
                    new InputStreamReader(
                            socket.getInputStream()));
            // Enable auto-flush:
            out = new PrintWriter(
                    new BufferedWriter(
                            new OutputStreamWriter(
                                    socket.getOutputStream())), true);
        } catch(IOException e) {
            // The socket should be closed on any
            // failures other than the socket
            // constructor:
            try {
                socket.close();
            } catch(IOException e2) {}
        }
        // Otherwise the socket will be closed by
        // the run() method of the thread.
    }

    public void Close() {
        try {
            socket.close();
        }catch (IOException e) {

        }
    }

    public void sendFile(String DirPath, String FileName) throws IOException {
        try {
            File file = new File(BackupInfo.REGIONDATA_DIR + File.separatorChar + FileName);
            dos = new DataOutputStream(socket.getOutputStream());
            if(file.exists()) {
                file_in = new FileInputStream(file);
                if(file_in == null) System.out.println("file_in is null");

                System.out.println("======== start to send file ========");
                byte[] bytes = new byte[1024];
                int length = 0;
                while((length = file_in.read(bytes, 0, bytes.length)) != -1) {
                    dos.write(bytes, 0, length);
                    dos.flush();
                }
                System.out.println();
                System.out.println("======== send file success ========");
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if(file_in != null)
                file_in.close();
            if(dos != null)
                dos.close();
        }
    }

    public void saveFile() {
        try {
            while(socket.getInputStream().available() != 0) {
                String FileName = in.readLine();

                dis = new DataInputStream(socket.getInputStream());
                // file name and length
                File file = new File(BackupInfo.REGIONDATA_DIR + File.separatorChar + FileName);
                file_out = new FileOutputStream(file);

                // start to get file
                byte[] bytes = new byte[1024];
                int length = 0;
                while((length = dis.read(bytes, 0, bytes.length)) != -1) {
                    file_out.write(bytes, 0, length);
                    file_out.flush();
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if(file_out != null)
                    file_out.close();
                if(dis != null)
                    dis.close();
            } catch (Exception e) {}
        }
    }

//    public void run() {
//        Thread t1 = new Thread(new RcvdServerMsg(this));
//        t1.start();
//        String[] command_line;
//        try {
//            while(running) {
//                Thread.sleep(Long.parseLong("1000"));
//                command_line = in.readLine().split(" ");
//                if (command_line[0] == "Upload") {
//                    sendFile(command_line[1], command_line[2]);
//                }else if (command_line[0] == "Get") {
//                    saveFile(command_line[1], command_line[2]);
//                }else {
//                    System.out.println("command form error");
//                }
//            }
//        }catch(Exception e) {
//            e.printStackTrace();
//        }
//        running = false;
//        try {
//            socket.close();
//        } catch(IOException e) {}
//    }
    public void deleteFile(String DirPath, String FileName) {
        try {
            Connect();
            String command = String.join(" ", "Delete", DirPath, FileName);
            sendMsg(command);
        }catch (Exception e) {

        }finally {
            Close();
        }
    }

    public void deleteLocalFile() {
        File directory = new File(BackupInfo.DEFAULT_DIR);
        File[] files = directory.listFiles();
        for (File f: files){
            if (f.exists() && f.isFile()){
                f.delete();
            }
        }
    }

    public void uploadFile(String DirPath, String FileName) {
        try {
            Connect();
            if(FileName.contains("_")) {
                sendFile(DirPath, FileName);
            }else {
                int prefix_len = FileName.length();
                File directory = new File(BackupInfo.REGIONDATA_DIR);
                String[] files = directory.list();
                for (String f: files){
                    if (f.substring(0,prefix_len).equals(FileName)){
                        sendFile(DirPath, f);
                    }
                }
            }
            sendFile(DirPath, FileName);
        }catch (IOException e) {

        }finally {
            Close();
        }
    }

    public void GetFile(String DirPath) {
        try {
            Connect();
            String command = String.join(" ", "Get", DirPath);
            sendMsg(command);
            saveFile();
        }catch (Exception e) {

        }finally {
            Close();
        }
    }
    public void sendMsg(String command) {
        out.write(command);
    }
}

//public class FileController {
//    protected RegionFileThread regionFileThread;
//    public FileController() throws IOException{
//        regionFileThread = new RegionFileThread();
//    }
//
//    synchronized public void deleteFile(String DirPath, String FileName) {
//        String command = String.join(" ", "Delete", DirPath, FileName);
//        regionFileThread.sendMsg(command);
//    }
//
//    synchronized public void uploadFile(String DirPath, String FileName) {
//        try {
//            regionFileThread.sendFile(DirPath, FileName);
//        }catch (IOException e) {
//
//        }
//    }
//
//    synchronized public void GetFile(String DirPath, String FileName) {
//        String command = String.join(" ", "Get", DirPath, FileName);
//        regionFileThread.sendMsg(command);
//    }
//}

