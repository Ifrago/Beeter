package edu.upc.eetac.dsa.ifrago.beeter.android;

import java.net.Authenticator;
import java.net.PasswordAuthentication;
import java.util.ArrayList;

import edu.upc.eetac.dsa.ifrago.beeter.android.api.BeeterAPI;
import edu.upc.eetac.dsa.ifrago.beeter.android.api.BeeterAndroidException;
import edu.upc.eetac.dsa.ifrago.beeter.android.api.Sting;
import edu.upc.eetac.dsa.ifrago.beeter.android.api.StingCollection;
import android.app.ListActivity;
import android.app.ProgressDialog;
import android.content.Context;
import android.content.Intent;
import android.content.SharedPreferences;
import android.os.AsyncTask;
import android.os.Bundle;
import android.util.Log;
import android.view.Menu;
import android.view.MenuItem;
import android.view.View;
import android.widget.ArrayAdapter;
import android.widget.ListView;
 
public class BeeterMainActivity extends ListActivity {
	
	private class FetchStingsTask extends AsyncTask<Void, Void, StingCollection> {
		private ProgressDialog pd;

		@Override
		protected StingCollection doInBackground(Void... params) {
			StingCollection stings = null;
			try {
				stings = BeeterAPI.getInstance(BeeterMainActivity.this).getStings();
			} catch (BeeterAndroidException e) {
				e.printStackTrace();
			}
			return stings;
		}

		@Override
		protected void onPostExecute(StingCollection result) {
			addStings(result);
			if (pd != null) {
				pd.dismiss();
			}
		}

		@Override
		protected void onPreExecute() {
			pd = new ProgressDialog(BeeterMainActivity.this);
			pd.setTitle("Searching...");
			pd.setCancelable(false);
			pd.setIndeterminate(true);
			pd.show();
		}

	}
	
	private final static String TAG = BeeterMainActivity.class.toString();
	/*private static final String[] items = { "lorem", "ipsum", "dolor", "sit",
			"amet", "consectetuer", "adipiscing", "elit", "morbi", "vel",
			"ligula", "vitae", "arcu", "aliquet", "mollis", "etiam", "vel",
			"erat", "placerat", "ante", "porttitor", "sodales", "pellentesque",
			"augue", "purus" };
	private ArrayAdapter<String> adapter;*/
	
	private ArrayList<Sting> stingList;
	private StingAdapter adapter;
	
	/** Called when the activity is first created. */
	@Override
	public void onCreate(Bundle savedInstanceState) {
		super.onCreate(savedInstanceState);
		setContentView(R.layout.beeter_layout);
		SharedPreferences prefs = getSharedPreferences("beeter-profile",
				Context.MODE_PRIVATE);
		final String username = prefs.getString("username", null);
		final String password = prefs.getString("password", null);
	 
		Authenticator.setDefault(new Authenticator() {
			protected PasswordAuthentication getPasswordAuthentication() {
				return new PasswordAuthentication(username, password
						.toCharArray());
			}
		});
		Log.d(TAG, "authenticated with " + username + ":" + password);
	 
		stingList = new ArrayList<>();
		adapter = new StingAdapter(this, stingList);
		setListAdapter(adapter);
		(new FetchStingsTask()).execute();
	}
	
	private void addStings(StingCollection stings){
		stingList.addAll(stings.getStings());//Introducimos los datos.
		adapter.notifyDataSetChanged();//Notificar que los datos han ssido modificados ( hay que refrescar la lista )
	}
	
	@Override
	protected void onListItemClick(ListView l, View v, int position, long id) {
		Sting sting = stingList.get(position);
		Log.d(TAG, sting.getLinks().get("self").getTarget());
		
		Intent intent = new Intent(this, StingDetailActivity.class);
		intent.putExtra("url", sting.getLinks().get("self").getTarget());
		startActivity(intent);
	}
	
	@Override
	public boolean onCreateOptionsMenu(Menu menu) {
		getMenuInflater().inflate(R.menu.beeter_actions, menu);
		return true;
	}
	 
	@Override
	public boolean onOptionsItemSelected(MenuItem item) {
		switch (item.getItemId()) {
		case R.id.miWrite:
			Intent intent = new Intent(this, WriteStingActivity.class);
			startActivity(intent);
			return true;
	 
		default:
			return super.onOptionsItemSelected(item);
		}
	 
	}
}



