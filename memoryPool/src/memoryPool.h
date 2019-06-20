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
#include <time.h>

#include<commons/log.h>
#include<commons/string.h>
#include<commons/config.h>
#include<readline/readline.h>
#include<commons/collections/node.h>
#include<commons/collections/list.h>
//#include <socketSaturados.h>



t_list* tablaMarcos;
t_list* tablaSegmentos;
t_list* tablaPaginas;
void* memoria;
int offsetMarco;
u_int16_t maxValue;
int cantMarcos;



typedef struct {
	int nroMarco;
	int estaLibre; //0 si libre 1 si ocupado
}marco;

typedef struct {
	char* nombreTabla;
	t_list* tablaPaginas;
}segmento;

typedef struct {
	int nroMarco; //como dirección lógica y después me desplazo en la memoria
	int modificado;
}pagina;


void mSelect(char* nombreTabla,u_int16_t key);
void mInsert(char* nombreTabla,u_int16_t key,char* valor);
void mCreate(char* nombreTabla, char* criterio, u_int16_t nroParticiones, long tiempoCompactacion );
void mDescribe();
void mDrop();
void mJournal();
void mGossip();



//AUXILIARES
void inicializar();
void mostrarMemoria();
segmento *crearSegmento(char* nombre);
segmento *buscarSegmento(char* nombre);
pagina *crearPagina();
void agregarPagina(segmento *seg, pagina *pag);
int primerMarcoLibre();
int hayMarcosLibres();
char* empaquetar(int operacion, long timestamp, u_int16_t key, char* value);
void agregarDato(long timestamp, u_int16_t key, char* value, pagina *pag);
char* conseguirValor(pagina* pNueva);
pagina *buscarPaginaConKey(segmento *seg, u_int16_t key);
pagina *pedirALissandraPagina(char* nombreTabla,u_int16_t key);
void pedirleCrearTablaAlissandra(char* nombretrable,char*criterio,u_int16_t nroParticiones,long tiempoCompactacion);
void pedirleALissandraQueBorre(char* nombreTabla);
void liberarMarco(int nroMarco);
t_log* init_logger();
t_config* read_config();

#endif /* MEMORYPOOL_H_ */
