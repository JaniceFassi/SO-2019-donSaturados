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
#include <semaphore.h>


#include<commons/log.h>
#include<commons/string.h>
#include<commons/config.h>
#include<readline/readline.h>
#include<commons/collections/node.h>
#include<commons/collections/list.h>
#include <socketSaturados.h>



//ESTRUCTURAS
typedef struct {
	int nroMarco;
	int estaLibre; //0 si libre 1 si ocupado
	pthread_mutex_t lockMarco;
	long ultimoUso;
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
	int multiprocesamiento;
	char** ipSeeds;
	char** puertoSeeds;
	int id;
}estructuraConfig;



//GOSSIPING
typedef struct {
    int nroMem; //cada proceso tendra un nroMem propio. Asignado a mano y unico.
    char* ip;
    int puerto;
    int activa; //1 si esta activa
}memorias;

//VARIABLES GLOBALES
t_log *logger;
char* pathConfig;
t_list* tablaMarcos;
t_list* tablaSegmentos;
t_list* marcosReemplazables;
t_list* memoriasConocidas;
t_list* semillas;
void* memoria;
int offsetMarco;
int fin;
u_int16_t maxValue;
int cantMarcos;
estructuraConfig *config;

//SEMAFOROS
pthread_mutex_t lockTablaSeg;
pthread_mutex_t lockTablaMarcos;
pthread_mutex_t lockTablaUsos;
pthread_mutex_t lockConfig;
pthread_mutex_t lockLRU;
sem_t lockTablaMem;
sem_t semJournal;
pthread_mutex_t lockJournal;



//LRU COMO DIOS MANDA
void actualizarLista();
void agregarAReemplazables(marco *marc);
void eliminarDeReemplazables(int nroMarco);
int getMarcoLibre();
int hayMarcosLibres();
int memoriaFull();
int mlru();


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
void init_configuracion(t_config* configuracion);
void prepararGossiping();
t_log* init_logger();
t_config* read_config();
segmento* crearSegmento(char* nombre);
pagina* crearPagina();
void agregarSegmento(segmento* nuevo);
void agregarPagina(segmento *seg, pagina *pag);
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
int esNumero(char *num);



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
u_int16_t handshakeConLissandra(char* ip, u_int16_t puerto);
int crearConexionLFS();

//MANEJAR MEMORIA
int memoriaLlena();
void liberarMarco(int nroMarco);
void agregarDato(long timestamp, u_int16_t key, char* value, pagina *pag);
void eliminarSegmento(segmento* nuevo);
void paginaDestroy(pagina* pagParaDestruir);
void segmentoDestroy(segmento* segParaDestruir);

//GOSSIPING
void prepararGossiping();
void desactivarMemoria(char *ip,int puerto);
void enviarMemorias(char *ip,int puerto);
void recibirMemorias(int cliente);
int memoriaActiva(char *ip, int puerto);
char *paqueteVerdadero();
char *empaquetarMemorias();
void pedirMemorias(int cliente);
void desempaquetarMemorias(char* paquete);
memorias *obtenerMemorias(char *ip,int puerto,int id);
bool existeMemoria(char *ip,int puerto,int id);
void liberarMemoria(memorias *victima);
memorias *crearMemoria(char *ip,int puerto, int id, int activa);
void mostrarActivas();

//AUX SECUNDARIAS
void* conseguirValor(pagina* pNueva);
void* conseguirTimestamp(pagina *pag);
void* conseguirKey(pagina *pag);
void mostrarMemoria();
void modificarConfig();
int verificarParametros(char **split,int cantParametros);

#endif /* MEMORYPOOL_H_ */
