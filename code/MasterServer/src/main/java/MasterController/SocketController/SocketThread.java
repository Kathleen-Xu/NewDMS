package MasterController.SocketController;

import MasterController.TableController;
import lombok.extern.slf4j.Slf4j;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;

//负责进行通信
@Slf4j
public class SocketThread extends Thread {
    private boolean isRunning = false;
    private CommandProcess ProcessCommand;
    public BufferedReader input = null;
    public PrintWriter output = null;

    public SocketThread(Socket socket, TableController tableController) throws IOException {
        this.ProcessCommand = new CommandProcess(tableController,socket);
        this.isRunning = true;

        this.input = new BufferedReader(new InputStreamReader(socket.getInputStream()));
        this.output = new PrintWriter(socket.getOutputStream(), true);

    }

    //@Override
    public void run() {
        String line;
        try {
            while (isRunning) {
                Thread.sleep(Long.parseLong("1000"));
                line = input.readLine();
                if (line != null) {
                    this.commandProcess(line);
                }
            }
        } catch (InterruptedException | IOException e) {
            e.printStackTrace();
        }

    }

    //转发信息
    public void sendMsg(String info) {
        output.println("M"+info.substring(1));
    }
    //调用命令处理
    public void commandProcess(String cmd) {
        log.warn(cmd);
        String result = "";
        result = ProcessCommand.commandProcess(cmd);
        if(!result.equals(""))
            this.sendMsg(result);
    }
}
