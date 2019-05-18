/*
 * socketSaturados.h
 *
 *  Created on: 18 abr. 2019
 *      Author: utnso
 */

#ifndef SOCKETSATURADOS_H_
#define SOCKETSATURADOS_H_
#include <stdio.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <sys/socket.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <arpa/inet.h>
#include <sys/socket.h>

struct sockaddr_in completServer(char* ipServer, int portServer);
int createSocket(u_int16_t *sock);
int conectClient(u_int16_t *sock,struct sockaddr_in direccionServidor);
int recivHandsake(u_int16_t sock,char*handshake);
int sendData(u_int16_t sock , const void *buffer ,int sizeBytes);
int linkClient(u_int16_t *sock,char* ipServer, int portServer);
int createServer(char* ipAddress,u_int16_t port, u_int16_t *server);
void listenForClients(int server, int cantConexiones);
int acceptConexion(int server,u_int16_t *socket_client,char* serverName,int handshake,u_int16_t value);
int recvData(u_int16_t socket, const void* buffer,int bytesToRecieve);

#endif /* SOCKETSATURADOS_H_ */
