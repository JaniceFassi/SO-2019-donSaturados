/*
 * socketSaturados.c
 *
 *  Created on: 18 abr. 2019
 *      Author: utnso
*/

#include "socketSaturados.h"
//ESTE CREA EL STRUCT DE ADRESSSERVER
void completServer(struct sockaddr_in adressServer,char* ipServer, int portServer){
	adressServer.sin_family = AF_INET;
	adressServer.sin_addr.s_addr = inet_addr(ipServer);
	adressServer.sin_port = htons(portServer);
}
//ESTE CREA EL SOCKET CLIENTE Y DEVULEVE EL NUMERO
int socketClient(){
	u_int16_t sock = socket(AF_INET, SOCK_STREAM, 0);
	if(sock<0){
		perror("no se pudo crear el socket");
	}
	return sock;
}

int conectClient(u_int16_t sock,struct sockaddr direccionServidor){
	if(connect(sock,(struct sockaddr *)&direccionServidor,sizeof(direccionServidor))!=0){
		perror("no se pudo conectar");
		return 1;
	}
	return 0;
}
char* recivHandsake(u_int16_t sock,char*handshake){//recive el handshake
	u_int16_t numbytes;
	if ((numbytes=recv(sock, handshake, sizeof(handshake), 0)) == -1) {
		perror("No pudo hacer recv de handshake");
		return 1;
	}
	handshake[numbytes]='\0';
	return handshake;
}
void sendMensjPepe(u_int16_t sock){
	char msj[11];
	printf("escriba el msj:\n");
	scanf("%s",msj);
	if(send(sock,msj,strlen(msj),0)<0){
		perror("no se pudo enviar");
	}
}

