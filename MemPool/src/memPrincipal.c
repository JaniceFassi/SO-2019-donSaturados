/*
 ============================================================================
 Name        : memPrincipal.c
 Author      : mpelozzi
 Version     :
 Copyright   : Your copyright notice
 Description : Hello World in C, Ansi-style
 ============================================================================
 */


#include "memPrincipal.h"

int main(void) {


	//SERVIDOR PARA KERNEL
	struct sockaddr_in servidorParaKernel;
	servidorParaKernel.sin_family = AF_INET;
	servidorParaKernel.sin_addr.s_addr = inet_addr("192.168.1.116");
	servidorParaKernel.sin_port = htons(7700);

	int servidorKernel= socket(AF_INET,SOCK_STREAM,0);

	if(servidorKernel==-1){
		perror("Error al crear socket\n");
		return 1;
		}

	int activado=1;

	if(setsockopt(servidorKernel, SOL_SOCKET, SO_REUSEADDR, (void*)&activado, (socklen_t)sizeof(activado))==-1){
		perror("Error al setear el socket\n");
		return 1;
		}


	if(bind(servidorKernel, (void*) &servidorParaKernel, sizeof(servidorParaKernel))!=0){
		perror("Fallo el bind\n");
		return 1;
			}


	listen(servidorKernel,100);
	printf("Servidor escuchando\n");

	struct sockaddr_in direccionCliente;
	unsigned int tamanioDireccion=sizeof(direccionCliente);

	int clienteKernel = accept(servidorKernel, (void*) &direccionCliente, (socklen_t*)&tamanioDireccion);


	if(clienteKernel<0){
		perror("Error al aceptar la conexion");
			}

	printf("Se conecto un cliente\n");


	//CLIENTE PARA LISSANDRA

	struct sockaddr_in servidorLissandra;

	servidorLissandra.sin_family = AF_INET;
	servidorLissandra.sin_addr.s_addr = inet_addr("192.168.1.116");
	servidorLissandra.sin_port = htons(7700);

	int lissandraServer = socket(AF_INET, SOCK_STREAM, 0);

	if(lissandraServer < 0){
		perror("No se pudo crear el socket cliente");
	}

	if(connect(lissandraServer, (struct sockaddr *) &servidorLissandra, sizeof(servidorLissandra)) !=0){
		perror("No se pudo conectar al servidor Lissandra");
		return 1;
		}





	int protocoloFuncion = 0;

	switch(protocoloFuncion){
		case 0:
			mSelect();
			break;
		case 1:
			mInsert();
			break;
		case 2:
			mCreate();
			break;
		case 3:
			mDescribe();
			break;
		case 4:
			mDrop();
			break;
		case 5:
			mJournal();
			break;


	}


	return EXIT_SUCCESS;
}



