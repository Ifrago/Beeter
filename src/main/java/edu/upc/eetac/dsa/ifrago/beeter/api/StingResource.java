package edu.upc.eetac.dsa.ifrago.beeter.api;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;

import javax.sql.DataSource;
import javax.ws.rs.BadRequestException;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.ForbiddenException;
import javax.ws.rs.GET;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.POST;
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

@Path("/stings")
// Todo lo que venga a la URL relativa "/sting" se come este Clase
// (StingResource)
public class StingResource {
	private DataSource ds = DataSourceSPA.getInstance().getDataSource();
	@Context
	private SecurityContext security;// Variable

	@GET
	@Produces(MediaType.BEETER_API_STING_COLLECTION)
	public StingCollection getStings(@QueryParam("length") int length,
			@QueryParam("before") long before, @QueryParam("after") long after) {
		StingCollection stings = new StingCollection();

		Connection conn = null;
		try {
			conn = ds.getConnection();// Conectamos con la base de datos
		} catch (SQLException e) {
			throw new ServerErrorException("Could not connect to the database",
					Response.Status.SERVICE_UNAVAILABLE);
		}

		PreparedStatement stmt = null;
		try {
			boolean updateFromLast = after > 0;
			stmt = conn.prepareStatement(buildGetStingsQuery(updateFromLast));// Para
																				// preparar
																				// la
																				// query
																				// con
																				// el
																				// metodo
																				// buildGetStingsQuery
																				// (
																				// metodo
																				// de
																				// abajo
																				// )
			if (updateFromLast) {
				stmt.setTimestamp(1, new Timestamp(after));// Construimos la
															// query, valor 1=
															// prmer
															// interrogante-> el
															// valor de este
															// será el segundo
															// argumento pasado
			} else {
				if (before > 0)
					stmt.setTimestamp(1, new Timestamp(before));// Construimos
																// la query,
																// valor 1=
																// prmer
																// interrogante->
																// el valor de
																// este será el
																// segundo
																// argumento
																// pasado
				else
					stmt.setTimestamp(1, null);// Construimos la query, no
												// ponemos nada en el ?. Nota:
												// updateFromLast
				length = (length <= 0) ? 5 : length;
				stmt.setInt(2, length);
			}
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

		return stings;
	}

	private String buildGetStingsQuery(boolean updateFromLast) {
		if (updateFromLast)
			return "select s.*, u.name from stings s, users u where u.username=s.username and s.last_modified > ? order by last_modified desc";
		else
			return "select s.*, u.name from stings s, users u where u.username=s.username and s.last_modified < ifnull(?, now())  order by last_modified desc limit ?";
	}

	@GET
	@Path("/{stingid}")
	@Produces(MediaType.BEETER_API_STING)
	public Response getSting(@PathParam("stingid") String stingid,
			@Context Request request) {
		// Create CacheControl
		CacheControl cc = new CacheControl();

		Sting sting = getStingFromDatabase(stingid);

		// Calculate the ETag on last modified date of user resource
		EntityTag eTag = new EntityTag(Long.toString(sting.getLastModified()));

		// Verify if it matched with etag available in http request
		Response.ResponseBuilder rb = request.evaluatePreconditions(eTag);

		// If ETag matches the rb will be non-null;
		// Use the rb to return the response without any further processing
		if (rb != null) {
			return rb.cacheControl(cc).tag(eTag).build();
		}

		// If rb is null then either it is first time request; or resource is
		// modified
		// Get the updated representation and return with Etag attached to it
		rb = Response.ok(sting).cacheControl(cc).tag(eTag);

		return rb.build();
	}

	private String buildGetStingByIdQuery() {
		return "select s.*, u.name from stings s, users u where u.username=s.username and s.stingid=?";
	}

	@POST
	@Consumes(MediaType.BEETER_API_STING)
	// necesita que lo que se le envie esté en el formato BEETER_API_STING
	@Produces(MediaType.BEETER_API_STING)
	public Sting createSting(Sting sting) {

		validateSting(sting);

		Connection conn = null;
		try {
			conn = ds.getConnection();
		} catch (SQLException e) {
			throw new ServerErrorException("Could not connect to the database",
					Response.Status.SERVICE_UNAVAILABLE);
		}
		PreparedStatement stmt = null;
		try {
			String sql = buildInsertSting();// Se genera la query
			stmt = conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);// Para
																				// ue
																				// me
																				// devuelva
																				// las
																				// Primary
																				// Key

			// stmt.setString(1, sting.getUsername());
			stmt.setString(1, security.getUserPrincipal().getName());
			stmt.setString(2, sting.getSubject());
			stmt.setString(3, sting.getContent());

			stmt.executeUpdate();// Ejecuto la actualización
			ResultSet rs = stmt.getGeneratedKeys();// query para saber si ha ido
													// bien la inserción
			if (rs.next()) {// Si ha ido bien me da la id del sting
				int stingid = rs.getInt(1);

				sting = getStingFromDatabase(Integer.toString(stingid));
			} else {
				// Something has failed...
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

		return sting;
	}

	private void validateSting(Sting sting) {
		if (sting.getSubject() == null)
			throw new BadRequestException("Subject can't be null.");
		if (sting.getContent() == null)
			throw new BadRequestException("Content can't be null.");
		if (sting.getSubject().length() > 100)
			throw new BadRequestException(
					"Subject can't be greater than 100 characters.");
		if (sting.getContent().length() > 500)
			throw new BadRequestException(
					"Content can't be greater than 500 characters.");
	}

	private String buildInsertSting() {
		return "insert into stings (username, subject, content) value (?, ?, ?)";// Query,
																					// acordarse
																					// que
																					// la
																					// id
																					// y
																					// lastmodified
																					// se
																					// autogeneran
	}

	private void validateUser(String stingid) {
		Sting currentSting = getStingFromDatabase(stingid);
		if (!security.getUserPrincipal().getName()
				.equals(currentSting.getUsername()))
			throw new ForbiddenException(
					"You are not allowed to modify this sting.");
	}

	@DELETE
	// Es muy parecido al GET, los diferencio por el metodo (este es DELETE)
	@Path("/{stingid}")
	public void deleteSting(@PathParam("stingid") String stingid) {// Este
																	// metodo no
																	// devuelve
																	// nada.
		Connection conn = null;
		validateUser(stingid);
		try {
			conn = ds.getConnection();
		} catch (SQLException e) {
			throw new ServerErrorException("Could not connect to the database",
					Response.Status.SERVICE_UNAVAILABLE);
		}

		PreparedStatement stmt = null;
		try {
			String sql = buildDeleteSting();
			stmt = conn.prepareStatement(sql);
			stmt.setInt(1, Integer.valueOf(stingid));

			int rows = stmt.executeUpdate();
			if (rows == 0)
				throw new NotFoundException("There's no sting with stingid="
						+ stingid);
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
	}

	private String buildDeleteSting() {
		return "delete from stings where stingid=?";
	}

	@PUT
	@Path("/{stingid}")
	@Consumes(MediaType.BEETER_API_STING)
	@Produces(MediaType.BEETER_API_STING)
	public Sting updateSting(@PathParam("stingid") String stingid, Sting sting) {
		validateUser(stingid);
		validateUpdateSting(sting);
		Connection conn = null;
		try {
			conn = ds.getConnection();
		} catch (SQLException e) {
			throw new ServerErrorException("Could not connect to the database",
					Response.Status.SERVICE_UNAVAILABLE);
		}

		PreparedStatement stmt = null;
		try {
			String sql = buildUpdateSting();
			stmt = conn.prepareStatement(sql);
			stmt.setString(1, sting.getSubject());
			stmt.setString(2, sting.getContent());
			stmt.setInt(3, Integer.valueOf(stingid));

			int rows = stmt.executeUpdate();
			if (rows == 1)
				sting = getStingFromDatabase(stingid);
			else {
				;// Updating inexistent sting
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

		return sting;
	}

	private void validateUpdateSting(Sting sting) {
		if (sting.getSubject() != null && sting.getSubject().length() > 100)
			throw new BadRequestException(
					"Subject can't be greater than 100 characters.");
		if (sting.getContent() != null && sting.getContent().length() > 500)
			throw new BadRequestException(
					"Content can't be greater than 500 characters.");
	}

	private String buildUpdateSting() {
		return "update stings set subject=ifnull(?, subject), content=ifnull(?, content) where stingid=?";
	}

	private Sting getStingFromDatabase(String stingid) {
		Sting sting = new Sting();

		Connection conn = null;
		try {
			conn = ds.getConnection();
		} catch (SQLException e) {
			throw new ServerErrorException("Could not connect to the database",
					Response.Status.SERVICE_UNAVAILABLE);
		}

		PreparedStatement stmt = null;
		try {
			stmt = conn.prepareStatement(buildGetStingByIdQuery());
			stmt.setInt(1, Integer.valueOf(stingid));
			ResultSet rs = stmt.executeQuery();
			if (rs.next()) {
				sting.setId(rs.getString("stingid"));
				sting.setUsername(rs.getString("username"));
				sting.setAuthor(rs.getString("name"));
				sting.setSubject(rs.getString("subject"));
				sting.setContent(rs.getString("content"));
				sting.setLastModified(rs.getTimestamp("last_modified")
						.getTime());
			} else {
				throw new NotFoundException("There's no sting with stingid ="
						+ stingid);
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

		return sting;
	}

	@GET
	// Metodo que utilizamos
	@Path("/search")
	// Path del subrecurso
	@Produces(MediaType.BEETER_API_STING_COLLECTION)
	// MediaType que utilizaremos
	public StingCollection searchByContentSubject(
			@QueryParam("subject") String subject, // Este metodo devuelve un
													// StingCollection
			@QueryParam("content") String content,
			@QueryParam("length") int length) throws SQLException { // Cogemos
																	// los
																	// QueryPrams

		// Nos conectamos en la base de
		// datos========================================
		StingCollection stings = new StingCollection();// Enviaremos esta
														// variable

		Connection conn = null;
		try {
			conn = ds.getConnection();// Conectamos con la base de datos
		} catch (SQLException e) {
			throw new ServerErrorException("Could not connect to the database",
					Response.Status.SERVICE_UNAVAILABLE);
		}

		String query = buildSearchByContentSubject(subject, content);
		PreparedStatement stmt = null;
		System.out.println("Recibimos:  subject: "+subject+" content= "+content+" length: "+length);
		try {
			System.out.println("Query a construir: "+query);
			stmt = conn.prepareStatement(buildSearchByContentSubject(subject, content));
			if (length != 0) {
				if (subject != null && content != null) {
					System.out.println("Estamos en Sub!=null, Cont!= null, length!=0");
					String subsend="'%"+subject+"%'";
					String contsend="'%"+content+"%'";
					stmt.setString(1, subsend);
					stmt.setString(2, contsend);
					stmt.setInt(3, length);// Limitamos el numero de resultados,
											// es el parametro 3
					System.out.println("'%"+subject+"%'"+ " "+"'%"+content+"%'"+" "+length);
				} else if (subject == null && content != null) {
					System.out.println("Estamos en Sub=null, Cont!= null, length!=0");
					stmt.setString(1, "'%"+content+"%'");
					stmt.setInt(2, length);// Limitamos el numero de resultados,
											// es el parametro 2
				} else if (subject != null && content == null) {
					System.out.println("Estamos en Sub=null, Cont= null, length!=0");
					stmt.setString(1, "'%"+subject+"%'");
					stmt.setInt(2, length);// Limitamos el numero de resultados,
											// es el parametro 2
				}
			} else if (length==0) {
				if (subject != null && content != null) {
					System.out.println("Estamos en Sub!=null, Cont!= null, length=0-> 5");
					stmt.setString(1, "'%"+subject+"%'");
					stmt.setString(2, "'%"+content+"%'");
					stmt.setInt(3, 5);// Limitamos el numero de resultados a 5,
										// es el parametro 3
				} else if (subject == null && content != null) {
					System.out.println("Estamos en Sub=null, Cont!= null, length=0-> 5");
					stmt.setString(1, "'%"+content+"%'");
					stmt.setInt(2, 5);// Limitamos el numero de resultados,
											// es el parametro 2
				} else if (subject != null && content == null) {
					System.out.println("Estamos en Sub!=null, Cont= null, length=0-> 5");
					stmt.setString(1, "'%"+subject+"%'");
					stmt.setInt(2, 5);// Limitamos el numero de resultados,
											// es el parametro 2
				}
			}
			
			System.out.println("Query salida: "+ stmt);
			ResultSet rs = stmt.executeQuery();//mandamos la query
			System.out.println("Resultado: "+ rs);
			while (rs.next()) {

				Sting sting = new Sting();// Para coger los valores de un sting
											// enconcreto

				sting.setAuthor(rs.getString("name"));
				System.out.println("Sting name: "+rs.getString("name"));
				sting.setContent(rs.getString("content"));
				System.out.println("Sting content: "+rs.getString("content"));
				sting.setId(rs.getString("stingid"));
				System.out.println("Sting stingid: "+rs.getString("stingid"));
				sting.setUsername(rs.getString("username"));
				System.out.println("Sting username: "+rs.getString("username"));
				sting.setLastModified(rs.getLong("last_modified"));
				System.out.println("Sting last_modified: "+rs.getLong("last_modified"));

				stings.addSting(sting);// añadimos el sting cogido de la base de
										// datos.
				System.out.println("Sting agregado!");

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
		return stings;
	}

	public String buildSearchByContentSubject(String subject, String content) {

		String query = null;
		if (subject != null && content != null)
			// QUERY: seleccionar toda la tabala sting y columna name de tabal
			// user donde aparezca algo del subject y del content que nos pasan.
			return "SELECT s.*, u.name FROM stings s, users u WHERE u.username=s.username AND subject LIKE ? OR content LIKE ? LIMIT ? ;";

		if (subject == null && content != null)
			// QUERY: seleccionar toda la tabala sting y columna name de tabal
			// user donde aparezca algo del content que nos pasan.
			return "SELECT s.*, u.name FROM stings s, users u WHERE u.username=s.username AND  content LIKE ? LIMIT ? ;";

		if (subject != null && content == null)
			// QUERY: seleccionar toda la tabala sting y columna name de tabal
			// user donde aparezca algo del subject que nos pasan.
			return "SELECT s.*, u.name FROM stings s, users u WHERE u.username=s.username AND subject LIKE ? LIMIT ? ;";

		if (subject == null && content == null)// En este caso no se puede
												// buscar nada ya que nos han
												// devuelto los dos paremotros
												// nulos.
			throw new BadRequestException("Se tiene que poner algo en el subject o context para poder buscar.");

		return query;
	}
}
