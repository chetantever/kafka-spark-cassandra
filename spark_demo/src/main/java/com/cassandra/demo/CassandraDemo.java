package com.cassandra.demo;

import java.util.Iterator;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Cluster.Builder;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;

public class CassandraDemo {
	private Cluster cluster; 
    private Session session;
 
    public void connect(String node, Integer port) {
        Builder b = Cluster.builder().addContactPoint(node);
        if (port != null) {
            b.withPort(port);
        }
        cluster = b.build();
        session = cluster.connect();
    }
 
    public Session getSession() {
        return this.session;
    }
 
    public void close() {
        session.close();
        cluster.close();
    }
    
    public static void main(String[] args) {
    	CassandraDemo client = new CassandraDemo();
    	client.connect("localhost", 9042);
    	Session session = client.getSession();
    	ResultSet result = session.execute("select * from java_api.system_metrics");
    	Iterator iterator = result.iterator();
    	while(iterator.hasNext()) {
    		System.out.println(iterator.next());
    	}
    	System.out.println("Done!");
    }
}
