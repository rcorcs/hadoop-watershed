

import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.exception.ZkInterruptedException;

public class ZkTest {
   public static void main(String []args) throws Exception {
      ZkClient zk = new ZkClient("localhost");
      CountDownLatch latch = new CountDownLatch(1);
      zk.subscribeChildChanges("/tmp", new IZkChildListener(){
         public void handleChildChange(String parentPath, List<String> currentChilds) throws Exception{
            System.out.println("Parent znode: "+parentPath);
            for(String znode: currentChilds){
               System.out.print(znode+";");
            }
            System.out.println();
         }
      });
      latch.await();
   }
}
