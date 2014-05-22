package edu.upc.eetac.dsa.ifrago.beeter.api;

import javax.naming.Context;			//Este codigo sirve para obtener solo una referencia al conectionPool
import javax.naming.InitialContext;		//Ya que el constructor es privado y solo se construirá una sola vez
import javax.naming.NamingException;	
import javax.sql.DataSource;

public class DataSourceSPA {
    private DataSource dataSource;
	private static DataSourceSPA instance;

	private DataSourceSPA() {
		super();
		Context envContext = null;
		try {
			envContext = new InitialContext();
			Context initContext = (Context) envContext.lookup("java:/comp/env");
			dataSource = (DataSource) initContext.lookup("jdbc/beeterdb");
		} catch (NamingException e1) {
			e1.printStackTrace();
		}
	}

	public final static DataSourceSPA getInstance() {
		if (instance == null)
			instance = new DataSourceSPA();
		return instance;
	}

	public DataSource getDataSource() {
		return dataSource;
	}
}
