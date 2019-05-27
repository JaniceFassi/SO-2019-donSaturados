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
	char* valor;
	u_int16_t keyTabla;
	u_int16_t modificado;
	u_int16_t timestamp;
}pagina;

typedef struct {
	char *nombreTabla;
	pagina pag[10];
}tabla; //las tablas son segmentos y las paginas son listas de structs con info

t_log* init_logger(void);
t_config* read_config(void);

void mSelect(char* nameTable,u_int16_t key);
void mInsert(char* nombreTabla,u_int16_t key,char* valor);
void mCreate();
void mDescribe();
void mDrop();
void mJournal();
void mGossip();
pagina *crearPagina(char* nombre,u_int16_t key,char* value);
pagina * buscarYreemplazar(u_int16_t ketTabla,char* valor);
tabla *buscarTabla(char *unNombre);
pagina *buscarPagina(tabla unaTabla, u_int16_t unaKey);
void pedirleALissandra(char *nombreTabla, u_int16_t unaKey);

#endif
