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
    private static Client client;
    private static Volume volume;
    final String VOLUME_NAME_1 = "myVolume";
    
    public void Setup() throws Exception {
        Options options = new Options();
        options.setPeriodicFileSizeUpdatesIntervalS(10);
        
        Logging.start(Logging.LEVEL_WARN, Logging.Category.all);
        
        userCredentials = UserCredentials.newBuilder().setUsername("enrico").addGroups("adm").build();
        
        String dirAddress = "192.168.1.7" + ":" + "32638";
        client = ClientFactory.createClient(dirAddress, userCredentials, null, options);
        client.start();
        
        volume = client.openVolume(VOLUME_NAME_1, null, options);       
        
    }
    
    /*
     * Create a file in xtreemfs
     */
    public void AddFile(String FileNamePath) throws Exception {
        // Open a file.
        FileHandle fileHandle = volume.openFile(
                userCredentials,
                FileNamePath,
                SYSTEM_V_FCNTL.SYSTEM_V_FCNTL_H_O_CREAT.getNumber()
                        | SYSTEM_V_FCNTL.SYSTEM_V_FCNTL_H_O_TRUNC.getNumber()
                        | SYSTEM_V_FCNTL.SYSTEM_V_FCNTL_H_O_RDWR.getNumber(), 0777);
      
        // Write to file.
        String data = "Need a testfile? Why not (\\|)(+,,,+)(|/)?";
        fileHandle.write(userCredentials, data.getBytes(), data.length(), 0);
        fileHandle.flush();         
        
        fileHandle.close();    
    }
    
    /*
     * Read a file in xtreemfs
     */
    public void ReadFile(String FileNamePath) throws Exception {
        // Open a file.
        FileHandle fileHandleRead = volume.openFile(
                userCredentials,
                "/"+FileNamePath,
                SYSTEM_V_FCNTL.SYSTEM_V_FCNTL_H_O_RDONLY.getNumber(), 0);
       // Get file attributes
        Stat stat = volume.getAttr(userCredentials, "/"+FileNamePath);
        log(stat.getSize());
        
        stat = volume.getAttr(userCredentials, FileNamePath);

        // Read from file.
        byte[] readData = new byte[(byte)stat.getSize()];
        int readCount = fileHandleRead.read(userCredentials, readData, (byte)stat.getSize(), 0);

        for (int i = 0; i < readCount; i++) {
           log(readData[i]);
        }

        fileHandleRead.close();        
    }
    
    /*
     * 
     */
    public void DeleteFile(String FileNamePath) throws Exception {
        
    }
    
    /*
     * 
     */
    public void ShutdownClient() throws Exception {
       client.shutdown(); 
    }
    
    /*
     * 
     */
    private static void log(Object aMsg){
        System.out.println(String.valueOf(aMsg));
    }
    
    
    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws Exception {
         Xtreemfsclient xt=new Xtreemfsclient();
         log("Initial Setup");
         //Initial Setup
         xt.Setup();
         log("Add file final_test.txt");
         //Add file
         xt.AddFile("final_test.txt");
         log("Read file final_test.txt");
         //Read file
         xt.ReadFile("final_test.txt");
         log("Exit.");
         //Exit
         xt.ShutdownClient();
    }
}
