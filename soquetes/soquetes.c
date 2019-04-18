/*
 * soquetes.c
 *
 *  Created on: 18 abr. 2019
 *      Author: mpelozzi
 */

#include "soquetes.h"

int crearServidor(){

	struct sockaddr_in direccionServidor;
			direccionServidor.sin_family = AF_INET;
			direccionServidor.sin_addr.s_addr = INADDR_ANY;
			direccionServidor.sin_port = 0; //Usa cualquier puerto que esté libre

	int servidor = socket(AF_INET, SOCK_STREAM, 0);

	//Para desocupar el puerto
	//Si da -1 es porque falló el intento de desocupar

	int ocp = 1;
	if(setsockopt(servidor, SOL_SOCKET, SO_REUSEADDR, &ocp, sizeof(int)) == -1){
		perror("No se pudo desocupar el puerto");
		exit(1);
			}

		//Bind, para anclar el socket al puerto al que va a escuchar

	if(bind(servidor, (void*) &direccionServidor, sizeof(direccionServidor))!=0){
		perror("Fallo el bind");
		return 1;
			}

	return servidor;


}


void escuchar(int servidor, int cantConexiones){
	if(listen(servidor, cantConexiones) == -1){
		perror("No se pudo escuchar correctamente");
	}
	printf("Estoy escuchando");
}


int aceptarConexion(int servidor){

	//creo un struct local para referenciar a los clientes que llegaron a la cola del listen
	struct sockaddr_in direccionCliente;
	int tamanioDireccion = sizeof(direccionCliente);

	int cliente = accept(servidor, (void *) &direccionCliente, &tamanioDireccion);

	if (cliente == -1){
		perror("Fallo el accept");
		return 1;
		}

	return cliente;
}

