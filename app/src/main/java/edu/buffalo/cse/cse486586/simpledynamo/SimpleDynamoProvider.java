package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.BufferedReader;
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
    static final int SERVER_PORT = 10000;

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
        // TODO Auto-generated method stub
        return 0;
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
        sendMessage(insDht, nodePortMap.get(hash));

        return null;
    }


    @Override
    public Cursor query(Uri uri, String[] projection, String selection,
                        String[] selectionArgs, String sortOrder) {
        // TODO Auto-generated method stub
        return null;
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
                        } else{
                            String replicationMessage = REPLICATE_KEY + delim + args[1] + delim + args[2];
                            sendMessage(replicationMessage, backupStorePorts[0]);
                        }
                    } else if (args[0].equals(REPLICATE_KEY)) {
                        localInsert(args[1], args[2]);
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

    private void replicateValues(String kv) {

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
