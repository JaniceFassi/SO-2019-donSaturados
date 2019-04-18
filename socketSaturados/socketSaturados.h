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

void completServer(struct sockaddr_in adressServer,char* ipServer, int portServer);
int socketClient();
int conectClient(u_int16_t sock,struct sockaddr direccionServidor);
char* recivHandsake(u_int16_t sock,char*handshake);
void sendMensjPepe(u_int16_t sock);


#endif /* SOCKETSATURADOS_H_ */
