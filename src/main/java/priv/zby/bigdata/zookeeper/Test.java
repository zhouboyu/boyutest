package priv.zby.bigdata.zookeeper;

import org.apache.log4j.Logger;
import org.apache.zookeeper.*;

import java.io.IOException;
import java.util.List;


/**
 * Created by zhouby on 17-7-13.
 */
public class Test {

    private static final Logger logger = Logger.getLogger(Test.class);

    public static void main(String [] args) throws IOException, InterruptedException {
        String parentPath= "/test"; //父节点
        String childrenPath = "/test/children"; //子节点

        ZKClient test = new ZKClient();
        //创建链接
        test.createConnection();

        /**
         * CreateMode.PERSISTENT
         */
        boolean isSuccess = test.createNode(parentPath, "abc", ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        if (isSuccess) {
            //读取数据
            String result = test.getNodeData(parentPath, true);
            logger.info("更新前数据：" + result);

            //更新数据
            isSuccess = test.updateNode(parentPath, String.valueOf(System.currentTimeMillis()));
            if(isSuccess){
                logger.info("更新后数据：" + test.getNodeData(parentPath, true));
            }

            // 创建子节点
            isSuccess = test.createNode(childrenPath, String.valueOf(System.currentTimeMillis()), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            if(isSuccess){
                test.updateNode(childrenPath, String.valueOf(System.currentTimeMillis()));
            }

            //读取子节点
            List<String> childrenList = test.getChildren(parentPath, true);
            if(childrenList!=null && !childrenList.isEmpty()){
                for(String children : childrenList){
                    System.out.println("子节点：" + children);
                }
            }
        }

        Thread.sleep(1000);
        //创建临时有序子节点
        test.createNode(childrenPath, String.valueOf(System.currentTimeMillis()), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

        test.createNode(childrenPath, String.valueOf(System.currentTimeMillis()), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);
        test.createNode(childrenPath, String.valueOf(System.currentTimeMillis()), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);
        test.createNode(childrenPath, String.valueOf(System.currentTimeMillis()), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);

        // 读取子节点，并删除
        List<String> childrenList = test.getChildren(parentPath, true);
        if (childrenList != null && !childrenList.isEmpty()) {
            for (String children : childrenList) {
                System.out.println("子节点：" + children);
                test.deleteNode(parentPath + "/" + children);
            }
        }

        //删除父节点
        if (test.exists(childrenPath, false) != null) {
            test.deleteNode(childrenPath);
        }

        //释放链接
        Thread.sleep(1000);
        test.releaseConnection();
    }

}
