#ifndef MEMORIAPRINCIPAL_H
#define MEMORIAPRINCIPAL_H


#include <stdio.h>
#include <stdlib.h>
#include <arpa/inet.h>
#include <sys/socket.h>

#include<commons/log.h>
#include<commons/string.h>
#include<commons/config.h>
#include<readline/readline.h>
#include<commons/collections/node.h>
#include<commons/collections/list.h>

typedef struct {
	char* nombre;
	u_int16_t keyTabla;
}Segmento;

t_log* init_logger(void);
t_config* read_config(void);

void mSelect(char* nameTable,u_int16_t key);
void mInsert(char*,u_int16_t,char*);
void mCreate();
void mDescribe();
void mDrop();
void mJournal();
void mGossip();

#endif
