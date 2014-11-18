package Util;

import java.io.Serializable;
import java.net.Socket;

public class Host implements Serializable {
	public final String hostname;
	public final int port;
	public final Socket socket = null;
	
	public Host(Socket socket){
		this.hostname = socket.getInetAddress().getHostName();
		this.port = socket.getPort();
	
	}
	
	public Host(String hostname, int port){
		this.hostname = hostname;
		this.port = port;
	}
	
	public String toString(){
		return "(" + hostname + "," + port + ")";
	}
	
	public boolean equals(Object o){
		if (!(o instanceof Host)){
			return false;
		}
		
		Host host = (Host) o;

        if(host.port != this.port)
            return false;


        return hostname.equalsIgnoreCase(host.hostname);
	}
	
	public int hashCode(){
		return hostname.toLowerCase().hashCode();
	}
}
