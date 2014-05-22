package edu.upc.eetac.dsa.ifrago.beeter.android.api;

import java.util.HashMap;
import java.util.Map;
 
public class Sting {
	private Map<String, Link> links = new HashMap<>();
	private String id;
	private String username;
	private String author;
	private String subject;
	private String content;
	private long lastModified;
 
	public String getId() {
		return id;
	}
 
	public void setId(String id) {
		this.id = id;
	}
 
	public String getUsername() {
		return username;
	}
 
	public void setUsername(String username) {
		this.username = username;
	}
 
	public String getAuthor() {
		return author;
	}
 
	public void setAuthor(String author) {
		this.author = author;
	}
 
	public String getSubject() {
		return subject;
	}
 
	public void setSubject(String subject) {
		this.subject = subject;
	}
 
	public String getContent() {
		return content;
	}
 
	public void setContent(String content) {
		this.content = content;
	}
 
	public long getLastModified() {
		return lastModified;
	}
 
	public void setLastModified(long lastModified) {
		this.lastModified = lastModified;
	}
 
	public Map<String, Link> getLinks() {
		return links;
	}
}