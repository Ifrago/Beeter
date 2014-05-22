package edu.upc.eetac.dsa.ifrago.beeter.api;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;

import javax.sql.DataSource;
import javax.ws.rs.BadRequestException;
import javax.ws.rs.Consumes;
import javax.ws.rs.ForbiddenException;
import javax.ws.rs.GET;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.ServerErrorException;
import javax.ws.rs.core.CacheControl;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.EntityTag;
import javax.ws.rs.core.Request;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;

import edu.upc.eetac.dsa.ifrago.beeter.api.model.Sting;
import edu.upc.eetac.dsa.ifrago.beeter.api.model.StingCollection;
import edu.upc.eetac.dsa.ifrago.beeter.api.model.User;
import edu.upc.eetac.dsa.ifrago.beeter.api.model.UserCollection;

@Path("/users")
public class UserResource {
	
	private static int numpag;
	private DataSource ds = DataSourceUsers.getInstance().getDataSource();
	private DataSource dstings = DataSourceSPA.getInstance().getDataSource();
	@Context
	private SecurityContext security;// Variable

	@GET
	@Path("/{username}")
	@Produces(MediaType.BEETER_API_STING)
	public Response getUser(@PathParam("username") String username,
			@Context Request request) {

		CacheControl cc = new CacheControl();

		User user = getUserFromDataBase(username);

		// Para la chaché, saber si ha sido modificado
		String s = user.getMail() + " " + user.getName();// Creamos un String
															// con los datos que
															// pueden ser
															// modificados
		EntityTag eTag = new EntityTag(Long.toString(s.hashCode()));// Creamos
																	// el eTag,
																	// a partir
																	// del
																	// String
																	// haciendo
																	// un hash

		Response.ResponseBuilder rb = request.evaluatePreconditions(eTag);// comparamos
																			// el
																			// eTag
																			// creado,
																			// con
																			// el
																			// que
																			// viene
																			// de
																			// la
																			// petición
																			// HTTP

		if (rb != null) {// Si el resultado no es nulo, significa que no ha sido
							// modificado el contenido ( o es la 1º vez )
			return rb.cacheControl(cc).tag(eTag).build();
		}

		// Si es nulo construimos la respuesta de cero.
		rb = Response.ok(user).cacheControl(cc).tag(eTag);

		return rb.build();

	}

	private User getUserFromDataBase(String username) {// Cogemos datos del
														// username
		User user = new User();

		// Hacemos la conexión a la base de datos
		Connection conn = null;
		try {
			conn = ds.getConnection();
		} catch (SQLException e) {
			throw new ServerErrorException("Could not connect to the database",
					Response.Status.SERVICE_UNAVAILABLE);
		}

		PreparedStatement stmt = null;
		try {
			stmt = conn.prepareStatement(buildGetUserByUsername());
			stmt.setString(1, username);
			ResultSet rs = stmt.executeQuery();
			if (rs.next()) {
				user.setUsername(rs.getString("username"));
				user.setName(rs.getString("name"));
				user.setMail(rs.getString("email"));

			} else {
				throw new NotFoundException("There's no sting with username ="
						+ username);
			}

		} catch (SQLException e) {
			throw new ServerErrorException(e.getMessage(),
					Response.Status.INTERNAL_SERVER_ERROR);
		} finally {
			try {
				if (stmt != null)
					stmt.close();
				conn.close();
			} catch (SQLException e) {

			}
		}
		return user;

	}

	public String buildGetUserByUsername() {
		return "SELECT *FROM users WHERE username=?;";
	}

	@PUT
	@Path("/{username}")
	@Consumes(MediaType.BEETER_API_USER)
	@Produces(MediaType.BEETER_API_USER)
	public User updateUser(@PathParam("username") String username, User user) {

		System.out.println("Dentro de UpdateUser");
		 validateUser(username);//Miramos que sea el usuario
		 System.out.println("User Validado");
		validateUpdateUser(user);// Miramos quelso campos que introduce sean de
									// tamaño correctos
		System.out.println("Atrbutos Validados");

		// Connexión con la base de datos
		Connection conn = null;
		try {
			conn = ds.getConnection();
		} catch (SQLException e) {
			throw new ServerErrorException("Could not connect to the database",
					Response.Status.SERVICE_UNAVAILABLE);
		}
		System.out.println("Hecha la connexión a BD");

		// Creamos la query
		PreparedStatement stmt = null;
		try {
			String sql = buildUpdateUser();
			System.out.println("Query escrita");
			stmt = conn.prepareStatement(sql);
			stmt.setString(1, user.getName());
			stmt.setString(2, user.getMail());
			stmt.setString(3, username);
			System.out.println("Query completada");

			int rows = stmt.executeUpdate();
			if (rows == 1)
				user = getUserFromDataBase(username);// Recogemos todos los
														// datos del Usuario

		} catch (SQLException e) {
			throw new ServerErrorException(e.getMessage(),
					Response.Status.INTERNAL_SERVER_ERROR);
		} finally {
			try {
				if (stmt != null)
					stmt.close();
				conn.close();
			} catch (SQLException e) {
			}
		}
		return user;
	}

	private void validateUser(String username) {
		
		if (!security.getUserPrincipal().getName().equals(username))
			throw new ForbiddenException(
					"You are not allowed to modify this sting.");
	}

	private void validateUpdateUser(User user) {
		if (user.getMail() != null && user.getMail().length() > 255)
			throw new BadRequestException(
					"Mail no puede ser mayor a 255 caracteres");
		if (user.getName() != null && user.getName().length() > 70)
			throw new BadRequestException("El Nombre no puede ser mayor a 70");
	}

	private String buildUpdateUser() {

		return " UPDATE users SET name=ifnull(?,name), email=ifnull(?,email) WHERE username=?;";
	}

	@GET
	@Produces(MediaType.BEETER_API_STING_COLLECTION)
	public StingCollection getStingsByUser() {

		StingCollection stings = new StingCollection();

		Connection conn = null;
		try {
			conn = ds.getConnection();// Conectamos con la base de datos
		} catch (SQLException e) {
			throw new ServerErrorException("Could not connect to the database",
					Response.Status.SERVICE_UNAVAILABLE);
		}

		PreparedStatement stmt = null;
		// try{

		// }

		return stings;
	}
	
	@GET
	@Path("/{username}/stings")
	@Produces(MediaType.BEETER_API_STING_COLLECTION)
	public StingCollection getStingsByUser(@PathParam("username") String username, @QueryParam("length") int length,
			@QueryParam("before") long before, @QueryParam("after") long after){
		StingCollection stings = new StingCollection ();
		
		System.out.println("Length: "+length+ " Before: "+before+" After: "+after);
		
		Connection conn = null;
		try {
			conn = dstings.getConnection();// Conectamos con la base de datos
		} catch (SQLException e) {
			throw new ServerErrorException("Could not connect to the database",
					Response.Status.SERVICE_UNAVAILABLE);
		}
		
		PreparedStatement stmt = null;

		try{
			
			boolean updateFromLast = after > 0;		
		stmt = conn.prepareStatement(buildGetStingByUser(updateFromLast));// Para preparar la query con el metodo buildGetStingsQuery ( metodo de abajo )
		stmt.setString(1, username);
		if (updateFromLast) {
			stmt.setTimestamp(2, new Timestamp(after));// Construimos la query, valor 1= prmer interrogante-> el valor de este será el segundo argumento pasado
		} else {
			if (before > 0)
				stmt.setTimestamp(2, new Timestamp(before));// Construimos/ la query, valor 1= prmer interrogante-> el valor de este será el segundo
															// argumento pasado
			else
				stmt.setTimestamp(2, null);// Construimos la query, no
											// ponemos nada en el ?. Nota:
											// updateFromLast
			length = (length <= 0) ? 5 : length;
			stmt.setInt(3, length);
				}
		System.out.println("Query que enviamos: "+ stmt);
		ResultSet rs = stmt.executeQuery();
		
		boolean first = true;
		long oldestTimestamp = 0;
		while (rs.next()) {
			Sting sting = new Sting();
			sting.setId(rs.getString("stingid"));
			sting.setUsername(rs.getString("username"));
			sting.setAuthor(rs.getString("name"));
			sting.setSubject(rs.getString("subject"));
			oldestTimestamp = rs.getTimestamp("last_modified").getTime();
			sting.setLastModified(oldestTimestamp);
			if (first) {
				first = false;
				stings.setNewestTimestamp(sting.getLastModified());
			}
			stings.addSting(sting);
		}
		stings.setOldestTimestamp(oldestTimestamp);
		
		}catch (SQLException e) {
			throw new ServerErrorException(e.getMessage(),
					Response.Status.INTERNAL_SERVER_ERROR);
		} finally {
			try {
				if (stmt != null)
					stmt.close();
				conn.close();
			} catch (SQLException e) {
			}
		}
		
		
		return stings;
	}

	private String buildGetStingByUser(boolean updateFromLast) {
		if(updateFromLast)
			return "SELECT s.* , u.name FROM stings s, users u WHERE u.username=s.username AND u.username= ? AND s.last_modified > ? order by last_modified desc;";
		else
			return "SELECT s.* , u.name FROM stings s, users u WHERE u.username=s.username AND u.username= ? AND s.last_modified < ifnull(?, now())  order by last_modified desc limit ?;";
	}
	

}
