/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package xtreemfsclient;

import com.esotericsoftware.minlog.Log;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
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
    final String VOLUME_NAME_1 = "demo";
    static final long MAX_FILE_SIZE = 10 * 1000 * 1024; //10MB limit - hopefully this 
    
    public void SetupClient() throws Exception {
        Options options = new Options();
        options.setPeriodicFileSizeUpdatesIntervalS(10);
        
        Logging.start(Logging.LEVEL_WARN, Logging.Category.all);
        
        userCredentials = UserCredentials.newBuilder().setUsername("enrico").addGroups("adm").build();
        
        String dirAddress = "demo.xtreemfs.org" + ":" + "32638";
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
      
        //Load file
        File f = new File(GetExecutionPath()+File.separator+   FileNamePath);
        
        // Write to file from string in memory
        //String data = "Need a testfile? Why not (\\|)(+,,,+)(|/)?";
        //fileHandle.write(userCredentials, data.getBytes(), data.length(), 0);
        //fileHandle.flush();
        
        // Write a file from file on real file system
        fileHandle.write(userCredentials, read(f), read(f).length, 0);
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
     * Shutdown Xtreemfs client 
     */
    public void ShutdownClient() throws Exception {
       client.shutdown(); 
    }
    
    /*
     * Very simple log
     */
    private static void log(Object aMsg){
        System.out.println(String.valueOf(aMsg));
    }
    
    /*
     * @return String path on jar file executor 
     */
    private String GetExecutionPath(){
        String absolutePath = getClass().getProtectionDomain().getCodeSource().getLocation().getPath();
        absolutePath = absolutePath.substring(0, absolutePath.lastIndexOf("/"));
        return absolutePath;
    }
    
    /*
     * Read file and return byte[]
     */
    private byte[] read(File file) throws IOException, FileTooBigException {

    if ( file.length() > MAX_FILE_SIZE ) {
        throw new FileTooBigException(file);
    }

    byte []buffer = new byte[(int) file.length()];
    InputStream ios = null;
    try {
        ios = new FileInputStream(file);
        if ( ios.read(buffer) == -1 ) {
            throw new IOException("EOF reached while trying to read the whole file");
        }        
    } finally { 
        try {
             if ( ios != null ) 
                  ios.close();
        } catch ( IOException e) {
        }
    }

    return buffer;
}
    
    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws Exception {
        
         Log.info("Setup xtreemfs client.");
         Xtreemfsclient xt=new Xtreemfsclient();
         //Initial Setup
         xt.SetupClient();
         Log.info("Add file www_xtreemfs_org.pdf");
         //Add file
         xt.AddFile("www_xtreemfs_org.pdf");
         //log.info("Read file www_xtreemfs_org.pdf");
         //Read file
         //xt.ReadFile("www_xtreemfs_org.pdf");
         //Delete file
         //log.info("Delete file: ");
         //xt.DeleteFile("www_xtreemfs_org.pdf");
         Log.info("Exit.");
         //Exit
         xt.ShutdownClient();
    }
}
