/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package xtreemfsclient;

import org.xtreemfs.common.libxtreemfs.Client;
import org.xtreemfs.common.libxtreemfs.ClientFactory;
import org.xtreemfs.common.libxtreemfs.FileHandle;
import org.xtreemfs.common.libxtreemfs.Options;
import org.xtreemfs.common.libxtreemfs.Volume;
import org.xtreemfs.foundation.logging.Logging;
import org.xtreemfs.foundation.pbrpc.generatedinterfaces.RPC.UserCredentials;
import org.xtreemfs.pbrpc.generatedinterfaces.GlobalTypes.SYSTEM_V_FCNTL;
import org.xtreemfs.pbrpc.generatedinterfaces.MRC.Stat;
        
/**
 *
 * @author Enrico
 */
public class Xtreemfsclient {
    
    private static UserCredentials userCredentials;
    final String VOLUME_NAME_1 = "myVolume";
    
    public void Setup() throws Exception {
        Options options = new Options();
        options.setPeriodicFileSizeUpdatesIntervalS(10);
        
        Logging.start(Logging.LEVEL_WARN, Logging.Category.all);
        
        userCredentials = UserCredentials.newBuilder().setUsername("enrico").addGroups("adm").build();
        
        String dirAddress = "192.168.1.7" + ":" + "32638";
        Client client = ClientFactory.createClient(dirAddress, userCredentials, null, options);
        client.start();
        
        Volume volume = client.openVolume(VOLUME_NAME_1, null, options);

        // Open a file.
        FileHandle fileHandle = volume.openFile(
                userCredentials,
                "/bla.txt",
                SYSTEM_V_FCNTL.SYSTEM_V_FCNTL_H_O_CREAT.getNumber()
                        | SYSTEM_V_FCNTL.SYSTEM_V_FCNTL_H_O_TRUNC.getNumber()
                        | SYSTEM_V_FCNTL.SYSTEM_V_FCNTL_H_O_RDWR.getNumber(), 0777);
        
        // Get file attributes
        Stat stat = volume.getAttr(userCredentials, "/bla.txt");
        //assertEquals(0, stat.getSize());

        // Write to file.
        String data = "Need a testfile? Why not (\\|)(+,,,+)(|/)?";
        fileHandle.write(userCredentials, data.getBytes(), data.length(), 0);
        fileHandle.flush();         
        
        //fileHandle.close();      
        client.shutdown();
    }

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws Exception {
         Xtreemfsclient xt=new Xtreemfsclient();
         xt.Setup();
    }
}
