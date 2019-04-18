/*
 * soquetes.h
 *
 *  Created on: 18 abr. 2019
 *      Author: utnso
 */

#ifndef SOQUETES_H_
#define SOQUETES_H_

#include <stdio.h>
#include <stdlib.h>
#include <arpa/inet.h>
#include <sys/socket.h>

#include<commons/log.h>
#include<commons/string.h>
#include<commons/config.h>
#include<readline/readline.h>

int crearServidor();

void escuchar(int servidor, int cantConexiones);

int aceptarConexion(int servidor);


#endif /* SOQUETES_H_ */
