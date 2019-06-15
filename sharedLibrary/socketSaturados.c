/*
 * socketSaturados.c
 *
 *  Created on: 18 abr. 2019
 *      Author: utnso
*/

#include "socketSaturados.h"
//ESTE CREA EL STRUCT DE ADRESSSERVER
struct sockaddr_in completServer(char* ipServer, int portServer){
	struct sockaddr_in adressServer;
	adressServer.sin_family = AF_INET;
	adressServer.sin_addr.s_addr = inet_addr(ipServer);
	adressServer.sin_port = htons(portServer);
	return adressServer;
}

//CREA EL SOCKET CLIENTE Y DEVULEVE 0 (BIEN) 1 (FALLO)
int createSocket(u_int16_t *sock){
	 *sock = socket(AF_INET, SOCK_STREAM, 0);
	if(sock<0){
		perror("no se pudo crear el socket");
		return 1;
	}
	return 0;
}

//CREA EL SOCKET SERVIDOR Y DEVULEVE 0 (BIEN) 1 (FALLO)
int createServer(char* ipAddress,u_int16_t port, u_int16_t* server){
	struct sockaddr_in serverAddress;
	serverAddress=completServer(ipAddress,(int)port);
	//int server = socket(AF_INET, SOCK_STREAM, 0);
	if(createSocket(server)!=0){
			return 1;
		}
	//Para desocupar el puerto
	//Si da -1 es porque falló el intento de desocupar

	int ocp = 1;
	if(setsockopt((int)*server, SOL_SOCKET, SO_REUSEADDR, &ocp, sizeof(ocp)) == -1){
		perror("No se pudo desocupar el puerto");
		return 1;
			}

		//Bind, para anclar el socket al puerto al que va a escuchar

	if(bind((int)*server, (void*) &serverAddress, sizeof(serverAddress))!=0){
		perror("Fallo el bind");
		return 1;
			}
	return 0;
}

//CONECTA EL CLIENTE Y DEVULEVE 0 (BIEN) 1 (FALLO)
int conectClient(u_int16_t *sock,struct sockaddr_in direccionServidor){
	if(connect((int) *sock,(struct sockaddr *)&direccionServidor,sizeof(direccionServidor))!=0){
		perror("No se pudo conectar");
		return 1;
	}
	return 0;
}

//CREA EL SOCKET CLIENTE, LO CONECTA Y HACE EL HANDSHAKE SI ES NECESARIO. DEVULEVE 0 (BIEN) 1 (FALLO)
int linkClient(u_int16_t *sock,char* ipServer, int portServer, u_int16_t id){
	struct sockaddr_in adressServer;
	adressServer= completServer(ipServer,portServer);

	if(createSocket(sock)!=0){
		return 1;
	}
	if(conectClient(sock,adressServer)!=0){
		return 1;
	}
		sendData(*sock, &id, sizeof(id));
	return 0;
}

//ACEPTA LA CONEXION, EN CASO DE QUE HAYA HANDSHAKE VERIFICA. dEVUELVE 0 (BIEN) 1 (FALLO)
int acceptConexion(int server,u_int16_t *socket_client,int idEsperado){

	//creo un struct local para referenciar a los clientes que llegaron a la cola del listen
	struct sockaddr_in clientAddress;
	u_int16_t client;
	u_int16_t addressSize = sizeof(clientAddress);
	u_int16_t idRecv=0;

	client = accept(server, (void *) &clientAddress, (socklen_t*)&addressSize);

	if (client == -1){
		perror("\nFallo el accept");
		return 1;
		}

	printf("\nAlguien se conecto\n");

		if(recvData(client,&idRecv,sizeof(u_int16_t))!=0){
		perror("\nNo se pudo recibir el handshake");
		return 1;
		}															//ARREGLAR. NO GUARDA BIEN EL ID EN idRecv. SI DEBUGUEAS TE DAS CUENTA

		if(idRecv!=idEsperado){
			perror("\nConexion denegada");
			return 1;
		}

	*socket_client = client;
	return 0;
}

int sendData(u_int16_t sock ,const void *buffer ,int sizeBytes){
	int bytesSend=0;
	int bytesReturn=0;
	while(bytesSend<sizeBytes){
	bytesReturn= send(sock,(void*)(buffer+ bytesSend) ,sizeBytes-bytesSend,0);
	if(bytesReturn<0){
		perror("no se pudo enviar");
		return 1;
		}
	bytesSend+=bytesReturn;
	}
	return 0;
}

int recvData(u_int16_t socket,const void* buffer,int bytesToRecieve){
	u_int16_t returnByte=0;
	u_int16_t byteRecv=0;

	while (byteRecv < (int)bytesToRecieve) {
	   returnByte = recv((int)socket, (void*)(buffer+byteRecv), bytesToRecieve-byteRecv, 0);
	   //Controlo Errores
	   if( returnByte <= 0 ) {
		  printf("Error al recibir Datos, solo se recibieron %d bytes de los %d bytes a recibir\n", byteRecv, (int)bytesToRecieve);
		  byteRecv = returnByte;
		  return 1;
	   }
	   //Si no hay problemas, sigo acumulando bytesEnviados
	   byteRecv+= returnByte;
	}

	return 0;
}

void listenForClients(int server, int cantConexiones){
	if(listen(server, cantConexiones) == -1){
		perror("No se pudo escuchar correctamente");
	}
	printf("Estoy escuchando");
}
