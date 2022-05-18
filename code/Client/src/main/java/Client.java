import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 *
 * @author : 徐凯琳
 * @version : 1.0
 * @Project : DMS
 * @Package : PACKAGE_NAME
 * @ClassName : Client.java
 * @createTime : 2022/5/13 16:52
 * @Email : 2628968409@qq.com
 * @Description :
 */
public class Client {
    public static void main(String[] args) throws IOException, InterruptedException {
        ClientProcessor clientProcessor = new ClientProcessor();
        clientProcessor.run();
    }
}
