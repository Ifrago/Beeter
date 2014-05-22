package edu.upc.eetac.dsa.ifrago.beeter.api;
import edu.upc.eetac.dsa.ifrago.beeter.api.model.BeeterError;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;

//Es un proveedor de mapeo de exceptiones.
@Provider 
public class WebApplicationExceptionMapper implements
		ExceptionMapper<WebApplicationException> {
	@Override
	public Response toResponse(WebApplicationException exception) {
		BeeterError error = new BeeterError(
				exception.getResponse().getStatus(), exception.getMessage());
		return Response.status(error.getStatus()).entity(error)
				.type(MediaType.BEETER_API_ERROR).build();
	}
 
}