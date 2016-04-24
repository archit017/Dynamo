package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Formatter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Semaphore;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

public class SimpleDynamoProvider extends ContentProvider {

    String TAG = "SimpleDynamoProvider";
    ArrayList<String> nodeHashList;
    HashMap<String, Integer> nodePortMap;
    Integer[] backupStorePorts;
    private int myPort;
    private String localNodeID;
    private String prevNodeID;

    private static String delim = "`";
    private static String INSERT_KEY = "InsertKeyValue";
    private static String REPLICATE_KEY = "ReplicateKeyValue";
    private static String QUERY_KEY = "QueryStuff";
    private static String QUERY_ALL = "QueryAlltheValues!!";
    private static String QUERY_RESULT = "AhaResult!";
    private static String DELETE_KEY = "DeleteKey!";
    private static String DELETE_ALL = "DeleteEverything";
    static final int SERVER_PORT = 10000;

    Semaphore waitForQuery = new Semaphore(0);
    private String queryResult;

    @Override
    public boolean onCreate() {
        TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
        Log.v(TAG, tel.getLine1Number());
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        myPort = (Integer.parseInt(portStr) * 2);
        localNodeID = genHash(String.valueOf(myPort / 2));
        Log.v(TAG, "PORT: " + myPort);

        try {
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        } catch (IOException e) {
            Log.e(TAG, "Can't create a ServerSocket");
            e.printStackTrace();
        }

        //Populate node IDs
        nodeHashList = new ArrayList<String>();
        nodePortMap = new HashMap<String, Integer>();
        int[] ports = {5554, 5556, 5558, 5560, 5562};
        for (int port :
                ports) {
            String hash = genHash(String.valueOf(port));
            nodeHashList.add(hash);
            nodePortMap.put(hash, Integer.valueOf(port * 2));
        }
        Collections.sort(nodeHashList);
        Log.v(TAG, "Node hash space" + nodeHashList);
        Log.v(TAG, "ports" + nodePortMap);

        //get next two neighbours
        backupStorePorts = new Integer[2];
        localNodeID = genHash(String.valueOf(myPort / 2));
        int i = nodeHashList.indexOf(localNodeID);
        prevNodeID = nodeHashList.get((i + 4) % 5);
        Log.v(TAG, "index " + i);
        nodePortMap.get(nodeHashList.get((i + 1) % 5));
        backupStorePorts[0] = nodePortMap.get(nodeHashList.get((i + 1) % 5));
        backupStorePorts[1] = nodePortMap.get(nodeHashList.get((i + 2) % 5));
        Log.v(TAG, "neighbours " + backupStorePorts[0] + " " + backupStorePorts[1]);
        return false;
    }

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        if (selection.equals("@")) {
            Log.v("DHTDel", "Local delete");
            deleteLocal();
        } else if (selection.equals("*")){
            String queryAllDht = DELETE_ALL + delim + myPort;
            int[] ports = {5554, 5556, 5558, 5560, 5562};
            for (int port :
                    ports) {
                sendMessage(queryAllDht, port*2);
            }
        }
        else {
            String deleteKey = DELETE_KEY + delim + selection;
            Log.v("Delete Result", deleteKey);
            String hash = findNode(genHash(selection));
            Log.v(TAG, "query : " + deleteKey + " " + hash + " key->" + genHash(selection));
            int index = nodeHashList.indexOf(hash);
            sendMessage(deleteKey, nodePortMap.get(nodeHashList.get(index)));
            index = (index+1)%5;
            sendMessage(deleteKey, nodePortMap.get(nodeHashList.get(index)));
            index = (index+1)%5;
            sendMessage(deleteKey, nodePortMap.get(nodeHashList.get(index)));
        }
        return 0; //TODO return what?
    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        Set<Map.Entry<String, Object>> contentSet = values.valueSet();
        String args[] = new String[2];  //args[0] value, args[1] key
        Iterator start = contentSet.iterator();
        int i = 0;
        while (start.hasNext()) {
            Map.Entry<String, Object> keypair = (Map.Entry<String, Object>) start.next();
            args[i++] = (String) keypair.getValue();
        }
        Log.v("insert", args[1] + " " + args[0]);

        String insDht = INSERT_KEY + delim + args[1] + delim + args[0];

        String hash = findNode(genHash(args[1]));
        Log.v("insert", "hash " + genHash(args[1]) + " " + hash);
//        String node = findNode(hash);
        //In case of head failure send Insert to backup store
        if (!sendMessage(insDht, nodePortMap.get(hash))) {
            int index = nodeHashList.indexOf(hash);
            index = (index + 1) % 5;
            sendMessage(insDht, nodePortMap.get(nodeHashList.get(index)));
        }

        return null;
    }


    @Override
    public Cursor query(Uri uri, String[] projection, String selection,
                        String[] selectionArgs, String sortOrder) {
        MatrixHelper cursorBuilder = null;
        //TODO implement rejoin as a query call. loop over local query results and check hash
        if (selection.equals("@")) {
            //TODO LDump
            Log.v("DHTQuery", "Local dump");
            String localValues = localQueryAll();
            Log.v("DHTQuery", "Local values " + localValues);
            if (localValues != null)
                cursorBuilder = new MatrixHelper(localValues);
            else
                return null;
        } else if (selection.equals("*")) {
            //TODO GDump
            String queryAllDht = QUERY_ALL + delim + myPort;
            sendMessage(queryAllDht, myPort);
            try {
                waitForQuery.acquire();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            if (queryResult.length() == 0) {
                return null;
            }
            Log.v("QUERY", queryResult + "|" + queryResult.length());
            cursorBuilder = new MatrixHelper(queryResult);
        } else {
            String queryDht = QUERY_KEY + delim + selection + delim + myPort;
            String hash = findNode(genHash(selection));
            Log.v(TAG, "query : " + queryDht + " " + hash + " key->" + genHash(selection));
            int index = nodeHashList.indexOf(hash);
            hash = nodeHashList.get((index + 2) % 5);
            if (!sendMessage(queryDht, nodePortMap.get(hash))) {
                index = (index + 1) % 5;
                sendMessage(queryDht, nodePortMap.get(nodeHashList.get(index)));
            }
            try {
                waitForQuery.acquire();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            Log.v("QueryResult", queryResult + " " + selection);
            if (queryResult == "")
                return null;
            cursorBuilder = new MatrixHelper(selection + delim + queryResult);
        }
        return cursorBuilder.cursor;
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection,
                      String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
    }

    private String genHash(String input) {
        MessageDigest sha1 = null;
        try {
            sha1 = MessageDigest.getInstance("SHA-1");
            byte[] sha1Hash = sha1.digest(input.getBytes());
            Formatter formatter = new Formatter();
            for (byte b : sha1Hash) {
                formatter.format("%02x", b);
            }
            return formatter.toString();
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        return null;
    }

    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {
        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];
            do {
                try {
                    Socket clientHook = serverSocket.accept();
                    BufferedReader reader = new BufferedReader(new InputStreamReader(clientHook.getInputStream()));
                    String message = reader.readLine();
                    Log.v(TAG, "Server " + message);
                    String[] args = message.split(delim);

                    if (args[0].equals(INSERT_KEY)) {
                        //TODO is the value mine?
                        String hash = genHash(args[1]);
                        localInsert(args[1], args[2]);
                        if (isInHashSpace(hash, prevNodeID, localNodeID)) {
                            String replicationMessage = REPLICATE_KEY + delim + args[1] + delim + args[2];
                            sendMessage(replicationMessage, backupStorePorts[0]);
                            sendMessage(replicationMessage, backupStorePorts[1]);
                        } else {
                            String replicationMessage = REPLICATE_KEY + delim + args[1] + delim + args[2];
                            sendMessage(replicationMessage, backupStorePorts[0]);
                        }
                    } else if (args[0].equals(REPLICATE_KEY)) {
                        localInsert(args[1], args[2]);
                    } else if (args[0].equals(QUERY_KEY)) {
                        localQuery(args[1], Integer.parseInt(args[2]));
                    } else if (args[0].equals(QUERY_ALL)) {
                        queryAll(message);
                    } else if (args[0].equals(QUERY_RESULT)) {
                        message = message.substring(message.indexOf(delim) + 1);
                        queryResult = message;
                        waitForQuery.release();
                    } else if (args[0].equals(DELETE_ALL)){
                        deleteLocal();
                    } else if(args[0].equals(DELETE_KEY)){
                        Log.v("Delete", "Delete local file " + args[1]);
                        File del = new File(getContext().getFilesDir().getAbsolutePath() + "/" + args[1]);
                        del.delete();
                    }
                    clientHook.close();
                    //serverSocket.close();
                } catch (IOException e) {
                    e.printStackTrace();
                    return null;
                }
            } while (true);
        }

        protected void onProgressUpdate(String... strings) {
            //Nothin to do here
        }
    }

    private void localInsert(String key, String value) {
        Log.v("Insert", key + " " + value);
        FileOutputStream key_store = null;
        try {
            key_store = getContext().openFileOutput(key, Context.MODE_PRIVATE);
            key_store.write(value.getBytes());
            key_store.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void localQuery(String key, Integer port) {
        FileInputStream key_retrieve = null;
        try {
            String message;
            key_retrieve = getContext().openFileInput(key);
            if (key_retrieve == null)
                message = null;
            else {
                BufferedReader buf = new BufferedReader(new InputStreamReader(key_retrieve));
                message = buf.readLine();
            }
            Log.v("query", "key " + key + " value " + message);
            String queryResult = QUERY_RESULT + delim + message;
            sendMessage(queryResult, port);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    void queryAll(String message){
        String[] args = message.split(delim);

        String localValues = localQueryAll();
        boolean fin=false;

        if(localValues!=null)
            message = message + delim + localValues.substring(0, localValues.lastIndexOf(delim));
        if(backupStorePorts[0] == Integer.parseInt(args[1])) {
            message = message.substring(message.indexOf(delim)+1);
            if(message.indexOf(delim)>=0){
                message = message.substring(message.indexOf(delim)+1);
            }else{
                message = "";
            }
            message = QUERY_RESULT + delim + message;
            fin = true;
        }
        //Failure handling
        if(!sendMessage(message,backupStorePorts[0])){
            if(!fin && backupStorePorts[1] == Integer.parseInt(args[1])) {
                message = message.substring(message.indexOf(delim)+1);
                if(message.indexOf(delim)>=0){
                    message = message.substring(message.indexOf(delim)+1);
                }else{
                    message = "";
                }
                message = QUERY_RESULT + delim + message;
                fin = true;
            }
            sendMessage(message, backupStorePorts[1]);
        }

    }

    private String localQueryAll() {
        File dir = getContext().getFilesDir();
        String[] files = dir.list();
        String queryAll = null;
        FileInputStream key_retrieve;
        String message;

        for (String file : files) {

            Log.v("LocalQuery", file);
            try {
                key_retrieve = getContext().openFileInput(file);
                if (key_retrieve == null)
                    message = null;
                else {
                    BufferedReader buf = new BufferedReader(new InputStreamReader(key_retrieve));
                    message = buf.readLine();
                    if (queryAll == null)
                        queryAll = file + delim + message + delim;
                    else
                        queryAll = queryAll + file + delim + message + delim;
                }
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return queryAll;
    }
    private void deleteLocal() {
        File dir = getContext().getFilesDir();
        File[] deleteKeys = dir.listFiles();
        for(File del:deleteKeys){
            Log.v("DeleteLocal", del.getName());
            del.delete();
        }
    }

    private boolean sendMessage(String message, Integer port) {
        try {
            Log.v(TAG, "Client sends: " + message + " to " + port);
            Socket join = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                    port);
            OutputStream out = (join.getOutputStream());
            //TODO failures
            byte[] byteStream = message.getBytes("UTF-8");
            out.write(byteStream);

            join.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return true;
    }

    private String findNode(String hash) {
        for (int i = 0; i < nodeHashList.size(); i++) {
            if (isInHashSpace(hash, nodeHashList.get((i + 4) % 5), nodeHashList.get(i))) {
                return nodeHashList.get(i);
            }
        }
        return null;
    }

    private boolean greaterThan(String first, String second) {
        //Returns true if first is greater than second
        return (first.compareTo(second) > 0);
    }

    private boolean isInHashSpace(String hash, String prevHash, String nodeHash) {
        if (greaterThan(hash, prevHash) && !greaterThan(hash, nodeHash)) {
            return true;
        } else if (!greaterThan(nodeHash, prevHash)) {
            return (!greaterThan(hash, nodeHash) && greaterThan(prevHash, hash))
                    || (greaterThan(hash, prevHash)) && greaterThan(hash, nodeHash);
        } else {
            return false;
        }
    }
}
