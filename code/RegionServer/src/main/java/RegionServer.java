import RegionController.RegionController;

/**
 * 在主节点注册的时候，节点名前缀为"Region_"，data 为 url
 */

public class RegionServer {
    public static void main(String[] args) throws Exception {
        RegionController regionController = new RegionController();
        regionController.run();
    }
}
