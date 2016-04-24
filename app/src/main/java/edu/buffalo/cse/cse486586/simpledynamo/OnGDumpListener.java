package edu.buffalo.cse.cse486586.simpledynamo;

import android.content.ContentResolver;
import android.database.Cursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.util.Log;
import android.view.View;
import android.widget.TextView;

/**
 * Created by girish on 4/1/16.
 */
public class OnGDumpListener implements View.OnClickListener {
    private final TextView mTextView;
    private final ContentResolver mContentResolver;
    private final Uri mUri;
    private static final String KEY_FIELD = "key";
    private static final String VALUE_FIELD = "value";

    public OnGDumpListener(TextView tv, ContentResolver contentResolver) {
        mTextView = tv;
        mContentResolver = contentResolver;
        mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledynamo.provider");
    }
    private Uri buildUri(String scheme, String authority) {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(authority);
        uriBuilder.scheme(scheme);
        return uriBuilder.build();
    }
    public void onClick(View v) {
        new Task().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR);
    }
    private class Task extends AsyncTask<Void, String, Void> {

        @Override
        protected Void doInBackground(Void... voids) {
            Cursor resultCursor = mContentResolver.query(mUri, null,
                    "*", null, null);
            int keyIndex = resultCursor.getColumnIndex(KEY_FIELD);
            int valueIndex = resultCursor.getColumnIndex(VALUE_FIELD);

            while(resultCursor.moveToNext()){
                String value = resultCursor.getString(keyIndex)+ " " + resultCursor.getString(valueIndex) + "\n";
                Log.v("LD", value);
                publishProgress(value);
            }
            return null;
        }

        @Override
        protected void onProgressUpdate(String... values) {
            mTextView.append(values[0]);
        }
    }
}
