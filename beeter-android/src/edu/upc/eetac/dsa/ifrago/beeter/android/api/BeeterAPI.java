package edu.upc.eetac.dsa.ifrago.beeter.android.api;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Map;
import java.util.Properties;
 



import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
 


import edu.upc.eetac.dsa.ifrago.beeter.android.MediaType;
import android.content.Context;
import android.content.res.AssetManager;
import android.util.Log;
 
public class BeeterAPI {
	private final static String TAG = BeeterAPI.class.getName();
	private static BeeterAPI instance = null;
	private URL url;
 
	private BeeterRootAPI rootAPI = null;
 
	private BeeterAPI(Context context) throws IOException,
			BeeterAndroidException {
		super();
 
		AssetManager assetManager = context.getAssets();
		Properties config = new Properties();
		config.load(assetManager.open("config.properties"));
		String serverAddress = config.getProperty("server.address");
		String serverPort = config.getProperty("server.port");
		url = new URL("http://" + serverAddress + ":" + serverPort
				+ "/beeter-api");
 
		Log.d("LINKS", url.toString());
		getRootAPI();
	}
 
	public final static BeeterAPI getInstance(Context context)
			throws BeeterAndroidException {
		if (instance == null)
			try {
				instance = new BeeterAPI(context);
			} catch (IOException e) {
				throw new BeeterAndroidException(
						"Can't load configuration file");
			}
		return instance;
	}
 
	private void getRootAPI() throws BeeterAndroidException {
		Log.d(TAG, "getRootAPI()");
		rootAPI = new BeeterRootAPI();
		HttpURLConnection urlConnection = null;
		try {
			urlConnection = (HttpURLConnection) url.openConnection();
			urlConnection.setRequestMethod("GET");
			urlConnection.setDoInput(true);
			urlConnection.connect();
		} catch (IOException e) {
			throw new BeeterAndroidException(
					"Can't connect to Beeter API Web Service");
		}
 
		BufferedReader reader;
		try {
			reader = new BufferedReader(new InputStreamReader(
					urlConnection.getInputStream()));
			StringBuilder sb = new StringBuilder();
			String line = null;
			while ((line = reader.readLine()) != null) {
				sb.append(line);
			}
 
			JSONObject jsonObject = new JSONObject(sb.toString());
			JSONArray jsonLinks = jsonObject.getJSONArray("links");
			parseLinks(jsonLinks, rootAPI.getLinks());
		} catch (IOException e) {
			throw new BeeterAndroidException(
					"Can't get response from Beeter API Web Service");
		} catch (JSONException e) {
			throw new BeeterAndroidException("Error parsing Beeter Root API");
		}
 
	}
 
	public StingCollection getStings() throws BeeterAndroidException {
		Log.d(TAG, "getStings()");
		StingCollection stings = new StingCollection();
 
		HttpURLConnection urlConnection = null;
		try {
			urlConnection = (HttpURLConnection) new URL(rootAPI.getLinks()
					.get("stings").getTarget()).openConnection();
			urlConnection.setRequestMethod("GET");
			urlConnection.setDoInput(true);
			urlConnection.connect();
		} catch (IOException e) {
			throw new BeeterAndroidException(
					"Can't connect to Beeter API Web Service");
		}
 
		BufferedReader reader;
		try {
			reader = new BufferedReader(new InputStreamReader(
					urlConnection.getInputStream()));
			StringBuilder sb = new StringBuilder();
			String line = null;
			while ((line = reader.readLine()) != null) {
				sb.append(line);
			}
 
			JSONObject jsonObject = new JSONObject(sb.toString());
			JSONArray jsonLinks = jsonObject.getJSONArray("links");
			parseLinks(jsonLinks, stings.getLinks());
 
			stings.setNewestTimestamp(jsonObject.getLong("newestTimestamp"));
			stings.setOldestTimestamp(jsonObject.getLong("oldestTimestamp"));
			JSONArray jsonStings = jsonObject.getJSONArray("stings");
			for (int i = 0; i < jsonStings.length(); i++) {
				Sting sting = new Sting();
				JSONObject jsonSting = jsonStings.getJSONObject(i);
				sting.setAuthor(jsonSting.getString("author"));
				sting.setId(jsonSting.getString("id"));
				sting.setLastModified(jsonSting.getLong("lastModified"));
				sting.setSubject(jsonSting.getString("subject"));
				sting.setUsername(jsonSting.getString("username"));
				jsonLinks = jsonSting.getJSONArray("links");
				parseLinks(jsonLinks, sting.getLinks());
				stings.getStings().add(sting);
			}
		} catch (IOException e) {
			throw new BeeterAndroidException(
					"Can't get response from Beeter API Web Service");
		} catch (JSONException e) {
			throw new BeeterAndroidException("Error parsing Beeter Root API");
		}
 
		return stings;
	}
 
	private void parseLinks(JSONArray jsonLinks, Map<String, Link> map)
			throws BeeterAndroidException, JSONException {
		for (int i = 0; i < jsonLinks.length(); i++) {
			Link link = SimpleLinkHeaderParser
					.parseLink(jsonLinks.getString(i));
			String rel = link.getParameters().get("rel");
			String rels[] = rel.split("\\s");
			for (String s : rels)
				map.put(s, link);
		}
	}
	
	public Sting getSting(String urlSting) throws BeeterAndroidException {
		Sting sting = new Sting();
	 
		HttpURLConnection urlConnection = null;
		try {
			URL url = new URL(urlSting);
			urlConnection = (HttpURLConnection) url.openConnection();
			urlConnection.setRequestMethod("GET");
			urlConnection.setDoInput(true);
			urlConnection.connect();
			BufferedReader reader = new BufferedReader(new InputStreamReader(
					urlConnection.getInputStream()));
			StringBuilder sb = new StringBuilder();
			String line = null;
			while ((line = reader.readLine()) != null) {
				sb.append(line);
			}
			JSONObject jsonSting = new JSONObject(sb.toString());
			sting.setAuthor(jsonSting.getString("author"));
			sting.setId(jsonSting.getString("id"));
			sting.setLastModified(jsonSting.getLong("lastModified"));
			sting.setSubject(jsonSting.getString("subject"));
			sting.setContent(jsonSting.getString("content"));
			sting.setUsername(jsonSting.getString("username"));
			JSONArray jsonLinks = jsonSting.getJSONArray("links");
			parseLinks(jsonLinks, sting.getLinks());
		} catch (MalformedURLException e) {
			Log.e(TAG, e.getMessage(), e);
			throw new BeeterAndroidException("Bad sting url");
		} catch (IOException e) {
			Log.e(TAG, e.getMessage(), e);
			throw new BeeterAndroidException("Exception when getting the sting");
		} catch (JSONException e) {
			Log.e(TAG, e.getMessage(), e);
			throw new BeeterAndroidException("Exception parsing response");
		}
	 
		return sting;
	}
	
	public Sting createSting(String subject, String content) throws BeeterAndroidException {
		Sting sting = new Sting();
		sting.setSubject(subject);
		sting.setContent(content);
		HttpURLConnection urlConnection = null;
		try {
			JSONObject jsonSting = createJsonSting(sting);
			URL urlPostStings = new URL(rootAPI.getLinks().get("create-stings")
					.getTarget());
			urlConnection = (HttpURLConnection) urlPostStings.openConnection();
			urlConnection.setRequestProperty("Accept",
					MediaType.BEETER_API_STING);
			urlConnection.setRequestProperty("Content-Type",
					MediaType.BEETER_API_STING);
			urlConnection.setRequestMethod("POST");
			urlConnection.setDoInput(true);
			urlConnection.setDoOutput(true);
			urlConnection.connect();
			PrintWriter writer = new PrintWriter(
					urlConnection.getOutputStream());
			writer.println(jsonSting.toString());
			writer.close();
			BufferedReader reader = new BufferedReader(new InputStreamReader(
					urlConnection.getInputStream()));
			StringBuilder sb = new StringBuilder();
			String line = null;
			while ((line = reader.readLine()) != null) {
				sb.append(line);
			}
			jsonSting = new JSONObject(sb.toString());
	 
			sting.setAuthor(jsonSting.getString("author"));
			sting.setId(jsonSting.getString("id"));
			sting.setLastModified(jsonSting.getLong("lastModified"));
			sting.setSubject(jsonSting.getString("subject"));
			sting.setContent(jsonSting.getString("content"));
			sting.setUsername(jsonSting.getString("username"));
			JSONArray jsonLinks = jsonSting.getJSONArray("links");
			parseLinks(jsonLinks, sting.getLinks());
		} catch (JSONException e) {
			Log.e(TAG, e.getMessage(), e);
			throw new BeeterAndroidException("Error parsing response");
		} catch (IOException e) {
			Log.e(TAG, e.getMessage(), e);
			throw new BeeterAndroidException("Error getting response");
		} finally {
			if (urlConnection != null)
				urlConnection.disconnect();
		}
		return sting;
	}
	 
	private JSONObject createJsonSting(Sting sting) throws JSONException {
		JSONObject jsonSting = new JSONObject();
		jsonSting.put("subject", sting.getSubject());
		jsonSting.put("content", sting.getContent());
	 
		return jsonSting;
	}
}