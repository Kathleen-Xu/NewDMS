package regionServer;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 *
 * @author : 徐凯琳
 * @version : 1.0
 * @Project : DMS
 * @Package : PACKAGE_NAME
 * @ClassName : regionServer.TestServer.java
 * @createTime : 2022/5/13 18:20
 * @Email : 2628968409@qq.com
 * @Description :
 */
public class TestServer {
    public static void main(String[] args) throws IOException {
        ServerProcessor serverProcessor = new ServerProcessor();
        serverProcessor.run();
    }
}


