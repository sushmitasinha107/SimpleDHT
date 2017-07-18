package edu.buffalo.cse.cse486586.simpledht;

import java.io.IOException;
import java.math.BigInteger;
import java.net.ServerSocket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Formatter;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;
import android.net.Uri;
import android.telephony.TelephonyManager;
import android.util.Log;

public class SimpleDhtProvider extends ContentProvider {



    class GroupMessengerProviderHelper extends SQLiteOpenHelper {

        public GroupMessengerProviderHelper(Context context, String name, SQLiteDatabase.CursorFactory factory, int version) {
            super(context, name, factory, version);
        }

        @Override
        public void onUpgrade(SQLiteDatabase db, int oldVersion, int newVersion) {
            onCreate(db);
        }

        @Override
        public void onCreate(SQLiteDatabase db) {
            db.execSQL("DROP TABLE IF EXISTS " + tableName);
            db.execSQL("CREATE TABLE  IF NOT EXISTS " + tableName + " (key TEXT, value TEXT);");

        }


    }

    public static GroupMessengerProviderHelper createDB;

    private static final String TAG = SimpleDhtProvider.class.getSimpleName();
    private static final int SERVER_PORT = 10000;

    public static ConcurrentHashMap<String, String> currMap = new ConcurrentHashMap<String, String>();
    public static String mNode;
    public static  String predNode;
    public static  String succNode;
    public static  int mPort;
    public static  int predPort;
    public static  int succPort;
    public static  MessagePojo mMessage = new MessagePojo(null, null, 0, 0, null,"");
    public static String tableName= "";

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub

        if (selection.equals("@") || selection.equals("*")) {

            //delete my map
            currMap = new ConcurrentHashMap<String, String>();


            if (selection.equals("*") && succPort != mPort) {

                //delete all maps
                ConcurrentHashMap<String,String> map = null;
                MessagePojo message1 = new MessagePojo(selection, null, mPort, 0, map, "delete");

               //Client.clientTask(succPort, selection, null, mPort, 0, "delete", map);

                Client.clientTask(succPort, message1);

            }
        } else {
            try {
                String hash = genHash(selection);

                BigInteger pred = new BigInteger(predNode, 16);
                BigInteger curr = new BigInteger(mNode, 16);
                BigInteger id = new BigInteger(hash, 16);

                if ((curr.compareTo(pred) == 0) || (pred.compareTo(curr) == 1  && id.compareTo(pred) == 1 && id.compareTo(curr) == 1 ) ||
                        (pred.compareTo(curr) == 1 && id.compareTo(pred) == -1 && id.compareTo(curr) <= 0) ||
                        (id.compareTo(pred) == 1 && id.compareTo(curr) < 1)){
                    //remove the specific key value from the map
                    currMap.remove(selection);
                    SQLiteDatabase database = SimpleDhtProvider.createDB.getWritableDatabase();
                    String [] selectionArgs1 ={SimpleDhtProvider.mNode};
                    database.delete(SimpleDhtProvider.tableName , "key = ?",selectionArgs1);

                    Log.e(TAG , "Removed key is " + selection);

                } else {

                    //key not present with me ; find with successor
                    ConcurrentHashMap<String,String> map = null;
                    MessagePojo m = new MessagePojo( selection, null, mPort, 0, map,"delete");
                    Client.clientTask(succPort,m);
                }
            } catch (NoSuchAlgorithmException e) {
                Log.e(TAG, "Hash creation Error in delete()" +e.getMessage());
            }
        }

        return 0;
    }



    @Override
    public Uri insert(Uri uri, ContentValues values) {
        // TODO Auto-generated method stub
        String key = values.getAsString("key");
        String value = values.getAsString("value");


        String hash = "";

        try {
             hash = genHash(key);
        } catch (NoSuchAlgorithmException e) {
            Log.e(TAG, "Hash creation Error in public Uri insert" +e.getMessage());
            e.printStackTrace();
        }

        BigInteger pred = new BigInteger(predNode, 16);
        BigInteger curr = new BigInteger(mNode, 16);
        BigInteger id = new BigInteger(hash, 16);

        if (
                (pred.compareTo(curr) == 1  && id.compareTo(pred) == 1 && id.compareTo(curr) == 1 ) ||
                (pred.compareTo(curr) == 1 && id.compareTo(pred) == -1 && id.compareTo(curr) <= 0) ||
                (id.compareTo(pred) == 1 && id.compareTo(curr) < 1)  || (curr.compareTo(pred) == 0)){

            //present at port where msg has to be inserted
            currMap.put(key, value);

            Log.e(TAG , "Inserted key is " + key + "at node  :"+ mNode);
           
            String [] selectionArgs ={values.getAsString("key")};

             SQLiteDatabase database = createDB.getWritableDatabase();
            Cursor cursor = database.query(tableName, null , "key = ?",selectionArgs,null,null,null);
            int count = cursor.getCount();
            if(count<1)
                database.insert(tableName, null ,values);
            else
                database.update(tableName ,values,"key = ?", selectionArgs );

        }
        else
        {
            ConcurrentHashMap<String, String> map = null;

            MessagePojo m = new MessagePojo(key, value, 0, 0,map, "insert");
            Client.clientTask(succPort, m);
        }


        return uri;
    }

    @Override
    public boolean onCreate() {
        // TODO Auto-generated method stub


        TelephonyManager tel = (TelephonyManager)this.getContext().getSystemService(
                Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        mPort = Integer.parseInt(portStr) * 2;

        tableName = "table_"+mPort;
        createDB = new GroupMessengerProviderHelper(getContext(),tableName,null,1);


        try {
            mNode = genHash(portStr);
        } catch (NoSuchAlgorithmException e) {
            Log.e(TAG, "Hash creation Error in onCreate()" +e.getMessage());
            e.printStackTrace();
            return false;
        }

        try {
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);

            Server.server(serverSocket);

        } catch (IOException e) {
            Log.e(TAG, "Socket Cannot be created in onCreate()" +e.getMessage());

            e.printStackTrace();
        }

        predNode = mNode;
        succNode = mNode;
        predPort = mPort;
        succPort = mPort;
        ConcurrentHashMap<String, String> map = null;
        if (mPort != 11108) {

            MessagePojo m = new MessagePojo( mNode, null, mPort, mPort, map,"join");
            Client.clientTask(11108,m);
        }
        return true;
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
            String sortOrder) {
        // TODO Auto-generated method stub
            if (selection.equals("*") || selection.equals("@")) {
                {
                    ConcurrentHashMap<String,String>  map = new ConcurrentHashMap<String, String>();
                    String cursorDeclaration[] = {"key", "value"};
                    MatrixCursor cursor = new MatrixCursor(cursorDeclaration);

                    if (selection.equals("@") || mPort == succPort) {

                        Set<String> keyMap = currMap.keySet();
                        for(String keyMapKey : keyMap) {
                            String cursorEntry[] = {keyMapKey, currMap.get(keyMapKey)};
                            cursor.addRow(cursorEntry);
                        }


                        SQLiteDatabase database = createDB.getWritableDatabase();
                        String [] selectionArgs1 ={selection};
                        Cursor curso = database.query(tableName, null , "key = ?",selectionArgs1,null,null,null);

                        Log.v("query", selection);
                        return cursor;
                    }
                    else
                    {
                        map.putAll(currMap);

                        MessagePojo m = new MessagePojo(selection, null, mPort, 0, map,"all");
                        Client.clientTask(succPort, m);
                        synchronized(mMessage) {
                            try {
                                mMessage.wait();
                            } catch (InterruptedException e) {
                                Log.e(TAG, "InterruptedException in query() SimpleDHTProvider" + e.getMessage());
                                e.printStackTrace();
                            }
                            Set<String> keyMap = mMessage.msgMap.keySet();

                            //populate the cursor with map value
                            for(String keyMapKey : keyMap){
                                String cursorEntry[] = {keyMapKey, mMessage.msgMap.get(keyMapKey)};
                                cursor.addRow(cursorEntry);
                            }

                            SQLiteDatabase database = createDB.getWritableDatabase();
                            String [] selectionArgs1 ={selection};
                            Cursor curso = database.query(tableName, null , "key = ?",selectionArgs1,null,null,null);

                            Log.v("query", selection);

                            ConcurrentHashMap<String,String>  nullMap = null;
                            mMessage.msgMap = nullMap;
                        }
                    }
                    return cursor;
                }
            }
            else
            {
                String cursorDeclaration[] = {"key", "value"};

                MatrixCursor cursor = new MatrixCursor(cursorDeclaration);


                try {
                    String hash = genHash(selection);
                    BigInteger pred = new BigInteger(predNode, 16);
                    BigInteger curr = new BigInteger(mNode, 16);
                    BigInteger id = new BigInteger(hash, 16);

                    if ( (pred.compareTo(curr) == 1  && id.compareTo(pred) == 1 && id.compareTo(curr) == 1 ) ||
                            (pred.compareTo(curr) == 1 && id.compareTo(pred) == -1 && id.compareTo(curr) <= 0) ||
                            (id.compareTo(pred) == 1 && id.compareTo(curr) < 1)  || (curr.compareTo(pred) == 0)){
                        String cursorEntry[] = {selection, currMap.get(selection)};
                        cursor.addRow(cursorEntry);

                        SQLiteDatabase database = createDB.getWritableDatabase();
                        String [] selectionArgs1 ={selection};
                        Cursor curso = database.query(tableName, null , "key = ?",selectionArgs1,null,null,null);

                        Log.v("query", selection);
                        return cursor;
                    } else {
                        ConcurrentHashMap<String, String> map = null;
                        MessagePojo m = new MessagePojo(selection, null, mPort, 0, map , "query");
                        Client.clientTask(succPort, m);
                        synchronized(mMessage) {
                            mMessage.wait();
                        }
                        String cursorEntry[] = {selection, mMessage.value};
                        cursor.addRow(cursorEntry);
                        mMessage.value = null;


                        SQLiteDatabase database = createDB.getWritableDatabase();
                        String [] selectionArgs1 ={selection};
                        Cursor curso = database.query(tableName, null , "key = ?",selectionArgs1,null,null,null);

                        Log.v("query", selection);
                    }
                } catch (NoSuchAlgorithmException e) {
                    Log.e(TAG, "Hash creation Error in query()" +e.getMessage());
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                return cursor;
            }
    }



    public static String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }


    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
    }


    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }


}
