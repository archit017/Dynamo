package edu.buffalo.cse.cse486586.simpledynamo;

import android.database.MatrixCursor;

/**
 * Created by girish on 2/11/16.
 */

public class MatrixHelper {
    private static final String[] KEY_VALUE_FIELD = {"key", "value"};
    public MatrixCursor cursor;
    private static String delim = "`";

    public MatrixHelper(String message) {
        cursor = new MatrixCursor(KEY_VALUE_FIELD);
        String[] args = message.split(delim);
        for(int i=0; i<args.length; i=i+2) {
            cursor.addRow(new Object[]{args[i], args[i+1]});
        }
    }
    public MatrixHelper(){
        cursor = new MatrixCursor(KEY_VALUE_FIELD);
    }
    public void addRow(String key, String value){
        cursor.addRow(new Object[]{key, value});
    }

}
