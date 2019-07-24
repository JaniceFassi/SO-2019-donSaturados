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
#include <unistd.h>
#include <sys/inotify.h>


#include<commons/log.h>
#include<commons/string.h>
#include<commons/config.h>
#include<readline/readline.h>
#include<commons/collections/node.h>
#include<commons/collections/list.h>
#include <socketSaturados.h>


//VARIABLES GLOBALES
t_log *logger;
char* pathConfig;
t_list* tablaMarcos;
t_list* tablaSegmentos;
t_list* listaDeUsos;
t_list* tablaMemActivas;
t_list* tablaMemActivasSecundaria; //tablas del gossiping
char** ipSeeds;
char** puertoSeeds; //variables del gossiping
int idMemoria;
void* memoria;
int offsetMarco;
u_int16_t maxValue;
int cantMarcos;
int posicionUltimoUso; // Lo usa el LRU
pthread_mutex_t lockTablaSeg;
pthread_mutex_t lockTablaMarcos;
pthread_mutex_t lockTablaUsos;
pthread_mutex_t lockInsert;
pthread_mutex_t lockSelect;
pthread_mutex_t lockDrop;
pthread_mutex_t lockConfig;


//ESTRUCTURA MEMORIA
typedef struct {
	int nroMarco;
	int estaLibre; //0 si libre 1 si ocupado
	pthread_mutex_t lockMarco;
}marco;

typedef struct {
	char* nombreTabla;
	t_list* tablaPaginas;
	pthread_mutex_t lockSegmento;
}segmento;

typedef struct {
	int nroMarco; //como dirección lógica y después me desplazo en la memoria
	int modificado;
}pagina;

typedef struct {
	char* ip;
	u_int16_t puerto;
	char* ipFS;
	int puertoFS;
	int retardoGossiping;
	int retardoJournal;
	int retardoMem;
	int retardoFS;
}estructuraConfig;


estructuraConfig *config;

//ESTRUCTURA LRU
typedef struct{
	int nroMarco;
	char *segmento;
	int posicionDeUso;
}posMarcoUsado;

//GOSSIPING
typedef struct {
    int nroMem; //cada proceso tendra un nroMem propio. Asignado a mano y unico.
    char* ip;
    char* puerto;
}infoMemActiva;

//API
char* mSelect(char* nombreTabla,u_int16_t key);
int mInsert(char* nombreTabla,u_int16_t key,char* valor);
int mCreate(char* nombreTabla, char* criterio, u_int16_t nroParticiones, long tiempoCompactacion );
char* mDescribe(char* nombreTabla);
int mDrop(char* nombreTabla);
int mJournal();
void mGossip();

void finalizar();
void eliminarMarcos();
void marcoDestroy(marco *unMarco);
void liberarSubstrings(char **liberar);
void destruirConfig();

//AUXILIARES DE ARRANQUE
int inicializar();
void prepararGossiping(t_config *configuracion);
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

//FUNCIONES PARA HILOS
void* consola(void* arg);
void* gestionarConexiones(void* arg);
void* recibirOperacion(int arg);
void* journalProgramado(void* arg);
void* gossipProgramado(void* arg);
void* correrInotify(void*arg);
void modificarConfig();
void* correrInotify(void*arg);



//AUXILIARES PARA LISSANDRA O KERNEL
char* empaquetar(int operacion, char* paquete);
char* formatearSelect(char* nombreTabla, u_int16_t key);
char* formatearInsert(char* nombreTabla, long timestamp, u_int16_t key, char* value);
char* formatearCreate(char* nombreTabla, char* consistencia, int particiones, long tiempoCompactacion);
char* selectLissandra(char* nombreTabla,u_int16_t key); //Devuelve el value
int insertLissandra(char* nombreTabla, long timestamp, u_int16_t key, char* value);
int createLissandra(char* nombreTabla,char*criterio,u_int16_t nroParticiones,long tiempoCompactacion);
char* describeLissandra(char* nombreTabla);
int dropLissandra(char* nombreTabla);
u_int16_t handshakeConLissandra(u_int16_t socket, char* ip, u_int16_t puerto);
int crearConexionLFS();

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
void actualizarListaDeUsos(int nroMarco);
bool estaModificada(pagina *pag);
int FULL();
int todosModificados(segmento* aux);

//GOSSIPING
void agregarMemActiva(int id,char* ip,char* puerto);
char* empaquetarTablaActivas();
char* formatearTablaGossip(int nro,char*ip,char*puerto);
void desempaquetarTablaSecundaria(char* paquete);
int pedirConfirmacion(char*ip,char* puerto);
char* confirmarActivo();
int estaRepetido(char*ip);
void agregarMemActiva(int id,char* ip,char*puerto);
int conseguirIdSecundaria();
void estaEnActivaElim(char*ip);
void actualizarArchConfig(char*ip,char*puerto);
void cargarInfoDeSecundaria();

//AUX SECUNDARIAS
int conseguirIndexSeg(segmento* nuevo);
void* conseguirValor(pagina* pNueva);
void* conseguirTimestamp(pagina *pag);
void* conseguirKey(pagina *pag);
void mostrarMemoria();
void modificarConfig();

#endif /* MEMORYPOOL_H_ */
