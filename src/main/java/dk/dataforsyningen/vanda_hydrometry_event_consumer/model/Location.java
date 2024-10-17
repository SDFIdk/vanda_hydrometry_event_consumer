package dk.dataforsyningen.vanda_hydrometry_event_consumer.model;

import java.util.Objects;

public class Location {

	Double x;
	
	Double y;
	
	String srid;
	
	public Double getX() {
		return x;
	}

	public void setX(Double x) {
		this.x = x;
	}

	public Double getY() {
		return y;
	}

	public void setY(Double y) {
		this.y = y;
	}

	public String getSrid() {
		return srid;
	}
	
	public int getSridAsInt() {
		int v = 0;
		try {
			v = Integer.parseInt(srid);
		} catch (NumberFormatException ex) {
			//Do nothing
		}
		return v;
	}

	public void setSrid(String srid) {
		this.srid = srid;
	}

	@Override
	public String toString() {
		return "Location [x=" + x + ", y=" + y + ", srid=" + srid + "]";
	}

	@Override
	public int hashCode() {
		return Objects.hash(srid, x, y);
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Location other = (Location) obj;
		return Objects.equals(srid, other.srid) && Objects.equals(x, other.x) && Objects.equals(y, other.y);
	}
	
	
}
