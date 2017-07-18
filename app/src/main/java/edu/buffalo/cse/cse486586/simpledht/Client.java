package edu.buffalo.cse.cse486586.simpledht;

import android.util.Log;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;


/**
 * Created by sushmitasinha on 4/10/17.
 */

public class Client extends Thread{
    private static final String TAG = Client.class.getSimpleName();
    public static void clientTask(final int port,final MessagePojo message){


        Thread t = new Thread()
        {
            public void run() {
                try {
                    Socket socket =
                            new Socket(InetAddress.getByAddress(new byte[] {10, 0, 2, 2}), port);
                    ObjectOutputStream objectOutputStream = new ObjectOutputStream(
                            socket.getOutputStream());
                    //MessagePojo message = new MessagePojo(key, value, pred, succ, map, type);
                    objectOutputStream.writeObject(message);
                    objectOutputStream.close();
                    socket.close();
                } catch (UnknownHostException e) {
                    Log.e(TAG, "Client + Send UnknownHostException"+port+" msgtype "+ message.msgType+ e.getMessage());
                } catch (IOException e) {
                    Log.e(TAG, "Client + Send socket IOException:\n" +port+" msgtype "+ message.msgType+ e.getMessage());
                }


            }
        };
        t.start();
    }

}
