package edu.upc.eetac.dsa.ifrago.beeter.android;

import edu.upc.eetac.dsa.ifrago.beeter.android.api.BeeterAPI;
import edu.upc.eetac.dsa.ifrago.beeter.android.api.BeeterAndroidException;
import edu.upc.eetac.dsa.ifrago.beeter.android.api.Sting;
import android.app.Activity;
import android.app.ProgressDialog;
import android.content.Intent;
import android.os.AsyncTask;
import android.os.Bundle;
import android.view.Menu;
import android.view.MenuItem;
import android.view.View;
import android.widget.EditText;
 
public class WriteStingActivity extends Activity {
	private final static String TAG = WriteStingActivity.class.getName();
 
	private class PostStingTask extends AsyncTask<String, Void, Sting> {
		private ProgressDialog pd;
 
		@Override
		protected Sting doInBackground(String... params) {
			Sting sting = null;
			try {
				sting = BeeterAPI.getInstance(WriteStingActivity.this)
						.createSting(params[0], params[1]);
			} catch (BeeterAndroidException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			return sting;
		}
 
		@Override
		protected void onPostExecute(Sting result) {
			showStings();
			if (pd != null) {
				pd.dismiss();
			}
		}
 
		@Override
		protected void onPreExecute() {
			pd = new ProgressDialog(WriteStingActivity.this);
 
			pd.setCancelable(false);
			pd.setIndeterminate(true);
			pd.show();
		}
	}
 
	@Override
	protected void onCreate(Bundle savedInstanceState) {
		super.onCreate(savedInstanceState);
		setContentView(R.layout.write_sting_layout);
 
	}
 
	public void cancel(View v) {
		finish();
	}
 
	public void postSting(View v) {
		EditText etSubject = (EditText) findViewById(R.id.etSubject);
		EditText etContent = (EditText) findViewById(R.id.etContent);
 
		String subject = etSubject.getText().toString();
		String content = etContent.getText().toString();
 
		(new PostStingTask()).execute(subject, content);
	}
 
	private void showStings() {
		Intent intent = new Intent(this, BeeterMainActivity.class);
		startActivity(intent);
	}
	

 
}