package com.unit.clean;

public class CatalogUnitTableJoin {
	private String id;
	private String typeCode;
	private String unit;
	private String provinceCode;
	private String city;
	private String rankCode;
	public CatalogUnitTableJoin(String id) {
		// TODO Auto-generated constructor stub
		this.id = id;
		typeCode = "null";
		unit = "null";
		provinceCode = "99";
		city = "null";
		rankCode = "999";
	}
	
	public String getId() {
		return id;
	}
	public void setId(String id) {
		this.id = id;
	}
	public String getTypeCode() {
		return typeCode;
	}
	public void setTypeCode(String typeCode) {
		this.typeCode = typeCode;
	}
	public String getUnit() {
		return unit;
	}
	public void setUnit(String unit) {
		this.unit = unit;
	}
	public String getProvinceCode() {
		return provinceCode;
	}
	public void setProvinceCode(String provinceCode) {
		this.provinceCode = provinceCode;
	}
	public String getCity() {
		return city;
	}
	public void setCity(String city) {
		this.city = city;
	}
	public String getRankCode() {
		return rankCode;
	}
	public void setRankCode(String rankCode) {
		this.rankCode = rankCode;
	}

	@Override
	public String toString() {
		String str = id + "'" + typeCode + "'" + unit + "'" + provinceCode + "'" + city + "'" + rankCode;
		System.out.println("----->result:"+str);
		return  str;
	}
}
