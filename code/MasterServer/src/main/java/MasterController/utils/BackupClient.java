package MasterController.utils;

import MasterController.SocketController.BackupInfo;

import java.io.*;

import java.net.Socket;

public class BackupClient {
    private Socket socket;
    private BufferedReader in;
    private PrintWriter out;

    public BackupClient() {
        try {
            socket = new Socket(BackupInfo.BACKUP_HOST,BackupInfo.PORT);
        } catch(IOException e) {

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
    }

    public void sendMsg(String command) {
        out.write(command);
    }
}
