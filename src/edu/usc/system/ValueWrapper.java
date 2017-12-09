package edu.usc.system;

public class ValueWrapper {
	public String value;
	public int config_num;

	public ValueWrapper(String value, int config_num) {
		this.value = value;
		this.config_num = config_num;
	}

	public String toString() {
		return "[" + value + ", " + config_num + "]";
	}
}
