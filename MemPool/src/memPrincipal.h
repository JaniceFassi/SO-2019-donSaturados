#ifndef MEMORIAPRINCIPAL_H
#define MEMORIAPRINCIPAL_H


#include <stdio.h>
#include <stdlib.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <string.h>

#include<commons/log.h>
#include<commons/string.h>
#include<commons/config.h>
#include<readline/readline.h>
#include<commons/collections/node.h>
#include<commons/collections/list.h>

t_log* init_logger(void);
t_config* read_config(void);

//Funciones API

void mSelect();
void mInsert(char* nombreTabla, u_int16_t keyTabla, char* valor);
void mCreate();
void mDescribe();
void mDrop();
void mJournal();
void mGossip();

//Estructura inicial de la memoria principal

typedef struct {
	int timestamp;
	u_int16_t key;
	char *value;
}Segmento;

typedef struct {
	Segmento segmento;
	struct Lista *siguiente;
}Lista;


#endif
