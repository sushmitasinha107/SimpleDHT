package edu.buffalo.cse.cse486586.simpledht;

import android.database.Cursor;
import android.database.MatrixCursor;
import android.database.sqlite.SQLiteDatabase;
import android.util.Log;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.OptionalDataException;
import java.io.StreamCorruptedException;
import java.math.BigInteger;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.NoSuchAlgorithmException;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by sushmitasinha on 4/10/17.
 */

public class Server extends Thread{

    private static final String TAG = Server.class.getName();
    public static void server(final ServerSocket s) {
        Thread t = new Thread() {
            public void run() {


                try {

                    ServerSocket serverSocket = s;
                    while (true) {

                        Socket socket = serverSocket.accept();
                        ObjectInputStream objectInputStream = new ObjectInputStream(socket.getInputStream());
                        MessagePojo message = (MessagePojo) objectInputStream.readObject();
                        String recType = message.msgType;
                        BigInteger pred = new BigInteger(SimpleDhtProvider.predNode, 16);
                        BigInteger curr = new BigInteger(SimpleDhtProvider.mNode, 16);

                        if ("join".equals(recType)) {

                            BigInteger id = new BigInteger(message.key, 16);


                            if ((curr.compareTo(pred) == 0) || (pred.compareTo(curr) == 1 && id.compareTo(pred) == 1 && id.compareTo(curr) == 1) ||
                                    (pred.compareTo(curr) == 1 && id.compareTo(pred) == -1 && id.compareTo(curr) <= 0) ||
                                    (id.compareTo(pred) == 1 && id.compareTo(curr) < 1)) {

                                if (SimpleDhtProvider.mPort == SimpleDhtProvider.predPort) {

                                    ConcurrentHashMap<String, String> map = null;
                                    MessagePojo m = new MessagePojo(SimpleDhtProvider.predNode, SimpleDhtProvider.succNode, SimpleDhtProvider.predPort, SimpleDhtProvider.succPort,map, "ack");

                                    Client.clientTask(message.predecessor, m);

                                    SimpleDhtProvider.predNode = message.key;
                                    SimpleDhtProvider.succNode = message.key;
                                    SimpleDhtProvider.predPort = message.predecessor;
                                    SimpleDhtProvider.succPort = message.successor;


                                } else {
                                    ConcurrentHashMap<String, String> map = null;

                                    MessagePojo m1 = new MessagePojo(message.key, null, 0, message.predecessor,map, "succ");
                                    Client.clientTask(SimpleDhtProvider.predPort, m1);
                                    MessagePojo m2 = new MessagePojo(SimpleDhtProvider.predNode, SimpleDhtProvider.mNode, SimpleDhtProvider.predPort, SimpleDhtProvider.mPort, map,"ack");
                                    Client.clientTask(message.predecessor, m2);
                                    SimpleDhtProvider.predNode = message.key;
                                    SimpleDhtProvider.predPort = message.predecessor;
                                }
                            } else {
                                MessagePojo m1 = new MessagePojo(message.key, message.value, message.predecessor, message.successor,message.msgMap, message.msgType);
                                Client.clientTask(SimpleDhtProvider.succPort,m1 );
                            }

                        } else if ("ack".equals(recType)) {


                            SimpleDhtProvider.predNode = message.key;
                            SimpleDhtProvider.succNode = message.value;
                            SimpleDhtProvider.predPort = message.predecessor;
                            SimpleDhtProvider.succPort = message.successor;


                        } else if ("succ".equals(recType)) {


                            SimpleDhtProvider.succNode = message.key;
                            SimpleDhtProvider.succPort = message.successor;

                        } else if ("insert".equals(recType)) {

                            String key = message.key;
                            String value = message.value;
                            try {
                                String hash = SimpleDhtProvider.genHash(key);
                                BigInteger id = new BigInteger(hash, 16);

                                if ((curr.compareTo(pred) == 0) || (pred.compareTo(curr) == 1 && id.compareTo(pred) == 1 && id.compareTo(curr) == 1) ||
                                        (pred.compareTo(curr) == 1 && id.compareTo(pred) == -1 && id.compareTo(curr) <= 0) ||
                                        (id.compareTo(pred) == 1 && id.compareTo(curr) < 1)) {
                                    SimpleDhtProvider.currMap.put(key, value);




                                } else {
                                    ConcurrentHashMap<String, String> map = null;

                                    MessagePojo m = new MessagePojo( key, value, 0, 0, map,"insert");

                                    Client.clientTask(SimpleDhtProvider.succPort,m);
                                }
                            } catch (NoSuchAlgorithmException e) {
                                Log.e(TAG, "Hash creation Error in server()" + e.getMessage());
                            }
                        } else if ("query".equals(recType)) {
                            if (SimpleDhtProvider.mPort == message.predecessor) {
                                SimpleDhtProvider.mMessage.value = message.value;

                                synchronized (SimpleDhtProvider.mMessage) {
                                    SimpleDhtProvider.mMessage.notify();
                                }
                            } else {

                                String cursorDeclaration[] = {"key", "value"};
                                MatrixCursor cursor = new MatrixCursor(cursorDeclaration);

                                String hash = null;
                                try {
                                    hash = SimpleDhtProvider.genHash(message.key);
                                } catch (NoSuchAlgorithmException e) {
                                    Log.e(TAG, "Hash creation Error in server()" + e.getMessage());
                                    e.printStackTrace();
                                }


                                BigInteger id = new BigInteger(hash, 16);

                                if ((curr.compareTo(pred) == 0) || (pred.compareTo(curr) == 1 && id.compareTo(pred) == 1 && id.compareTo(curr) == 1) ||
                                        (pred.compareTo(curr) == 1 && id.compareTo(pred) == -1 && id.compareTo(curr) <= 0) ||
                                        (id.compareTo(pred) == 1 && id.compareTo(curr) < 1)) {
                                    if (SimpleDhtProvider.mPort == message.predecessor) {
                                        String cursorEntry[] = {message.key, SimpleDhtProvider.currMap.get(message.key)};
                                        cursor.addRow(cursorEntry);

                                        SQLiteDatabase database = SimpleDhtProvider.createDB.getWritableDatabase();
                                        String [] selectionArgs1 ={SimpleDhtProvider.mNode};
                                        Cursor cursors = database.query(SimpleDhtProvider.tableName, null , "key = ?",selectionArgs1,null,null,null);

                                    } else {
                                        ConcurrentHashMap<String, String> map = null;
                                        MessagePojo m = new MessagePojo(null, SimpleDhtProvider.currMap.get(message.key), message.predecessor, 0, map,"query");
                                        Client.clientTask(message.predecessor, m);
                                    }
                                } else {
                                    ConcurrentHashMap<String, String> map = null;

                                    MessagePojo m = new MessagePojo(message.key, null, message.predecessor, 0,map, "query");
                                    Client.clientTask(SimpleDhtProvider.succPort, m);
                                }
                            }
                        } else if ("all".equals(recType)) {
                            if (SimpleDhtProvider.mPort == message.predecessor) {
                                SimpleDhtProvider.mMessage.msgMap = message.msgMap;
                                synchronized (SimpleDhtProvider.mMessage) {
                                    SimpleDhtProvider.mMessage.notify();
                                }
                            } else {
                                int origin = message.predecessor;
                                String cursorDeclaration[] = {"key", "value"};
                                MatrixCursor cursor = new MatrixCursor(cursorDeclaration);

                                if (message.key.equals("@") || SimpleDhtProvider.mPort == SimpleDhtProvider.succPort) {
                                    Set<String> keyMap = SimpleDhtProvider.currMap.keySet();
                                    for (String keyMapKey : keyMap) {
                                        String cursorEntry[] = {keyMapKey, SimpleDhtProvider.currMap.get(keyMapKey)};
                                        cursor.addRow(cursorEntry);
                                    }

                                    SQLiteDatabase database = SimpleDhtProvider.createDB.getWritableDatabase();
                                    String [] selectionArgs1 ={SimpleDhtProvider.mNode};
                                    Cursor curso = database.query(SimpleDhtProvider.tableName, null , "key = ?",selectionArgs1,null,null,null);




                                } else {
                                    message.msgMap.putAll(SimpleDhtProvider.currMap);

                                    MessagePojo m =  new MessagePojo(message.key, null, origin, 0,  message.msgMap , "all");
                                    Client.clientTask(SimpleDhtProvider.succPort, m);
                                }
                            }
                        } else if ("delete".equals(recType)) {
                            if (message.key.equals("*") || message.key.equals("@")) {

                                //delete everything from current map
                                SimpleDhtProvider.currMap = new ConcurrentHashMap<String, String>();
                                // more than one node present and we have to delete all keys
                                if (message.key.equals("*") && SimpleDhtProvider.succPort != message.predecessor) {
                                    ConcurrentHashMap<String, String> map = null;
                                    //delete everything from all map
                                    MessagePojo m = new MessagePojo( message.key, null, message.predecessor, 0, map, "delete");
                                    Client.clientTask(SimpleDhtProvider.succPort,m);
                                }
                            } else {


                                 //delete specific node's key
                                try {
                                    String hash = SimpleDhtProvider.genHash(message.key);
                                    BigInteger id = new BigInteger(hash, 16);

                                    //if key present with me
                                    if ((pred.compareTo(curr) == 1 && id.compareTo(pred) == 1 && id.compareTo(curr) == 1) ||
                                            (pred.compareTo(curr) == 1 && id.compareTo(pred) == -1 && id.compareTo(curr) <= 0) || (curr.compareTo(pred) == 0) ||
                                            (id.compareTo(pred) == 1 && id.compareTo(curr) < 1)) {

                                        SimpleDhtProvider.currMap.remove(message.key);



                                        SQLiteDatabase database = SimpleDhtProvider.createDB.getWritableDatabase();
                                        String [] selectionArgs1 ={SimpleDhtProvider.mNode};

                                        int curso = database.delete(SimpleDhtProvider.tableName , "key = ?",selectionArgs1);

                                    } else {

                                        //if looking for key with successor
                                        ConcurrentHashMap<String, String> map = null;
                                        MessagePojo m =  new MessagePojo( message.key, null, message.predecessor, 0, map,"delete");

                                        Client.clientTask(SimpleDhtProvider.succPort,m);
                                    }
                                } catch (NoSuchAlgorithmException e) {
                                    Log.e(TAG, "Hash creation Error in server()" + e.getMessage());
                                }
                            }
                        }
                    }
                } catch (OptionalDataException e) {

                    Log.e(TAG, "OptionalDataException in server()" + e.getMessage());
                    e.printStackTrace();
                } catch (StreamCorruptedException e) {
                    Log.e(TAG, "StreamCorruptedException in server()" + e.getMessage());
                    e.printStackTrace();
                } catch (IOException e) {
                    Log.e(TAG, "IOException in server()" + e.getMessage());
                    e.printStackTrace();
                } catch (ClassNotFoundException e) {
                    Log.e(TAG, "ClassNotFoundException in server()" + e.getMessage());
                    e.printStackTrace();
                }
            }
        };

        t.start();

    }

}
