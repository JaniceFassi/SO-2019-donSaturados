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
#include <socketSaturados.h>


//VARIABLES GLOBALES
t_list* tablaMarcos;
t_list* tablaSegmentos;
t_list* listaDeUsos;
void* memoria;
int offsetMarco;
u_int16_t maxValue;
int cantMarcos;
int posicionUltimoUso; // Lo usa el LRU


//ESTRUCTURA MEMORIA
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

//ESTRUCTURA LRU
typedef struct{
	int nroMarco;
	int posicionDeUso;
}posMarcoUsado;

//API
void mSelect(char* nombreTabla,u_int16_t key);
void mInsert(char* nombreTabla,u_int16_t key,char* valor);
void mCreate(char* nombreTabla, char* criterio, u_int16_t nroParticiones, long tiempoCompactacion );
void mDescribe();
void mDrop(char* nombreTabla);
void mJournal();
void mGossip();

void finalizar();
void eliminarMarcos();
void marcoDestroy(marco *unMarco);

//AUXILIARES DE ARRANQUE
void inicializar();
t_log* init_logger();
t_config* read_config();
segmento* crearSegmento(char* nombre);
pagina* crearPagina();
posMarcoUsado* crearPosMarcoUsado(int nroMarco,int pos);
void agregarSegmento(segmento* nuevo);
void agregarPagina(segmento *seg, pagina *pag);
void agregarPosMarcoUsado(posMarcoUsado* nuevo);
pagina* buscarPaginaConKey(segmento *seg, u_int16_t key);
segmento* buscarSegmento(char* nombre);

//AUXILIARES PARA LISSANDRA O KERNEL
char* empaquetar(int operacion, char* paquete);
char* pedirALissandraPagina(char* nombreTabla,u_int16_t key); //Devuelve el value
void pedirleCrearTablaAlissandra(char* nombretrable,char*criterio,u_int16_t nroParticiones,long tiempoCompactacion);
void pedirleALissandraQueBorre(char* nombreTabla);

//MANEJAR MEMORIA
int memoriaLlena();
int primerMarcoLibre();
void liberarMarco(int nroMarco);
void agregarDato(long timestamp, u_int16_t key, char* value, pagina *pag);
void eliminarSegmento(segmento* nuevo);
void paginaDestroy(pagina* pagParaDestruir);
void segmentoDestroy(segmento* segParaDestruir);
int LRU();
void agregarListaUsos(int nroMarco);
void eliminarDeListaUsos(int nroMarcoAEliminar);

//AUX SECUNDARIAS
int conseguirIndexSeg(segmento* nuevo);
char* conseguirValor(pagina* pNueva);
void mostrarMemoria();

#endif /* MEMORYPOOL_H_ */
