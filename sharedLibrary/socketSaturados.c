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
//ESTE CREA EL SOCKET CLIENTE Y DEVULEVE EL NUMERO
int createSocket(u_int16_t *sock){
	 *sock = socket(AF_INET, SOCK_STREAM, 0);
	if(sock<0){
		perror("no se pudo crear el socket");
		return 1;
	}
	return 0;
}

int conectClient(u_int16_t *sock,struct sockaddr_in direccionServidor){
	if(connect((int) *sock,(struct sockaddr *)&direccionServidor,sizeof(direccionServidor))!=0){
		perror("no se pudo conectar");
		return 1;
	}
	return 0;
}
int recivHandsake(u_int16_t sock,char*handshake){//recibe el handshake
	u_int16_t numbytes;
	if ((numbytes=recv(sock, handshake, sizeof(handshake), 0)) == -1) {
		perror("No pudo hacer recv de handshake");
		return 1;
	}
	handshake[numbytes]='\0';
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
int linkClient(u_int16_t *sock,char* ipServer, int portServer){
	struct sockaddr_in adressServer;
	adressServer= completServer(ipServer,portServer);

	if(createSocket(sock)!=0){
		return 1;
	}
	if(conectClient(sock,adressServer)!=0){
			return 1;
	}
	return 0;
}


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


void listenForClients(int server, int cantConexiones){
	if(listen(server, cantConexiones) == -1){
		perror("No se pudo escuchar correctamente");
	}
	printf("Estoy escuchando");
}


int acceptConexion(int server,u_int16_t *socket_client,char* serverName,int handshake,u_int16_t value){

	//creo un struct local para referenciar a los clientes que llegaron a la cola del listen
	struct sockaddr_in clientAddress;
	u_int16_t client;
	u_int16_t addressSize = sizeof(clientAddress);

	client = accept(server, (void *) &clientAddress, (socklen_t*)&addressSize);

	if (client == -1){
		perror("Fallo el accept");
		return 1;
		}
	printf("Alguien se conecto\n");
	char message [50]="Hola te conectaste con ";
	strcat(message,serverName);
	if (handshake!=0){
		if(send(client,message,strlen(message),0)==-1){ //PASAR POR PARAMETRO EL NOMBRE DEL SERVIDOR
			perror("Error al enviar handshake");
			return 1;
		}
/*		if(send(client,value,sizeof(value),0)==-1){ //PASAR POR PARAMETRO EL NOMBRE DEL SERVIDOR
			perror("Error al enviar handshake");
			return 1;
		}*/
	}
	*socket_client = client;
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

