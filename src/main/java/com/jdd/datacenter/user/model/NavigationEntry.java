package com.jdd.datacenter.user.model;

import java.io.Serializable;
import java.util.Map;
/**
 * 
 * @author hadoop
 *
 */
public class NavigationEntry implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	private String userId;
	private String pageType;
	@SuppressWarnings("rawtypes")
	private Map otherData;
	
	@SuppressWarnings("rawtypes")
	public NavigationEntry(String userId, String pageType, Map otherData) {
		super();
		this.userId = userId;
		this.pageType = pageType;
		this.otherData = otherData;
	}

	public String getUserId() {
		return userId;
	}

	public void setUserId(String userId) {
		this.userId = userId;
	}

	public String getPageType() {
		return pageType;
	}

	public void setPageType(String pageType) {
		this.pageType = pageType;
	}

	@SuppressWarnings("rawtypes")
	public Map getOtherData() {
		return otherData;
	}

	@SuppressWarnings("rawtypes")
	public void setOtherData(Map otherData) {
		this.otherData = otherData;
	}
	
	public String toString(){
		String ret = "User: " + userId + " navigating a " + pageType;
		if(otherData != null){
			ret += " page with " + otherData.toString();
		}
		return ret;
	}
}
