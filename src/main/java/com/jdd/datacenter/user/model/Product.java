package com.jdd.datacenter.user.model;

import java.io.Serializable;

public class Product implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private long id;
	private String title;
	private double price;
	private String category;
	
		
	public Product() {
		super();
	}

	public Product(long id, String title, double price, String category) {
		super();
		this.id = id;
		this.title = title;
		this.price = price;
		this.category = category;
	}
	
	public long getId() {
		return id;
	}
	public void setId(long id) {
		this.id = id;
	}
	public String getTitle() {
		return title;
	}
	public void setTitle(String title) {
		this.title = title;
	}
	public double getPrice() {
		return price;
	}
	public void setPrice(double price) {
		this.price = price;
	}
	public String getCategory() {
		return category;
	}
	public void setCategory(String category) {
		this.category = category;
	}
	
	public boolean equals(Object obj){
		if(obj == null){
			return false;
		}
		if(obj instanceof Product){
			if(this == obj){
				return true;
			}
			return ((Product) obj).getId() == this.id;
		}
		
		return false;
	}
	
	public int hashCode(){
		return (int)(id % Integer.MAX_VALUE);
	}
	
	public String toString(){
		return "id: " + id + " title: " + title + " price: " + price;
	}
	
}
