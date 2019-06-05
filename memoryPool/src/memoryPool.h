/*
 * memoryPool.h
 *
 *  Created on: 3 jun. 2019
 *      Author: utnso
 */

#ifndef MEMORYPOOL_H_
#define MEMORYPOOL_H_


#include <stdio.h>
#include <stdlib.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <pthread.h>
#include <string.h>

#include<commons/log.h>
#include<commons/string.h>
#include<commons/config.h>
#include<readline/readline.h>
#include<commons/collections/node.h>
#include<commons/collections/list.h>

typedef struct {
	long timestamp;
	u_int16_t key;
	char* value;
}datoTabla;

typedef struct {
	void* inicio;
	int offset;
	int modificado;
}marco;

typedef struct {
	char* nombreTabla;
	t_list* tablaPaginas;
}segmento;


typedef struct {
	int nroPagina; //??
	marco* pagina;
}pagina;

t_list* tablaMarcos;
t_list* tablaSegmentos;
t_list* tablaPaginas;




void mSelect(char* nombreTabla,u_int16_t key);
void mInsert(char* nombreTabla,u_int16_t key,char* valor);
void mCreate();
void mDescribe();
void mDrop();
void mJournal();
void mGossip();


//AUXILIARES

segmento *crearSegmento(char* nombre);
segmento *buscarSegmento(char* nombre);
pagina *crearPagina();
void agregarPagina(segmento *seg);



#endif /* MEMORYPOOL_H_ */
