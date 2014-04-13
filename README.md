
Para poder utilizar el programa, hace falta poner en marcha el servidor tomcat.
Para implementar la realización de búsquedas en los campos subject y content de los
stings la URI relativa es:
/stings/search?subject={subject}&content={content}&length={length}

Para poder acceder a los perfiles de los usuarios hay que poner la URI:
http://localhost:8080/beeter-api/users/{username} con método GET
Para poder editar el perfil de usuario, hace falta estar autenticado, hay que poner la URI:
http://localhost:8080/beeter-api/users/{username} con método PUT
Para poder ver los stings de unusuario especifico, hay que utilizar la URI:
http://localhost:8080/beeter-api/users/{username}/stings con método GET
