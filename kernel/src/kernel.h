/*
 * kernel.h
 *
 *  Created on: 18 abr. 2019
 *      Author: utnso
 */

#ifndef KERNEL_H_
#define KERNEL_H_
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <readline/history.h>
#include<commons/log.h>
#include<commons/string.h>
#include<commons/config.h>
#include <commons/log.h>
#include<readline/readline.h>
#include<commons/collections/node.h>
#include<commons/collections/list.h>
#include<commons/collections/queue.h>
#include<socketSaturados.h>
#include<pthread.h>
#include<semaphore.h>
#include<sys/time.h>

struct metricas{
	double tiempoS;
	double tiempoI;
	int cantS;
	int cantI;
};

struct metricas metrica;

t_log* logger;
t_config* config;
t_log* init_logger(void);
t_config* read_config(void);
int quantum;

// lista de memorias que me van a pasar
struct memoria{
	u_int16_t id;
	int estado; // para saber si esta ocupada
	int puerto; // al ser varias memorias me deberia conectar con cada una?
	char *ip;
};
t_list *memorias;
// lista de las memorias para cada criterio
t_list *criterioSC;
t_list *criterioSHC;
t_list *criterioEC;

//los script son para la cola de ready
struct script{
	int id;
	char *input;     // puede ser el path o la linea en caso de venir x consola
	int lineasLeidas; // es para saber donde posicionarse al terminar el quantum
	int estado;			  // para saber si fallo o no (no se si va)
	int modoOp;     // aca seteo si es un request o si es un script entero, 0 para script desde archivo, 1 para script de una sola linea
};

int idScriptGlobal=0;
// cola de new
t_queue *new;
// cola de ready
t_queue *ready;
// cola de exec
t_queue *exec;
// cola de exit
t_queue *myExit;

struct metadataTabla{
	char *table;
	char *consistency;
	u_int16_t numPart;
	long compTime;
};

t_list *listaMetadata;
int terminaHilo=0;
int main();
void run(char * path);
int parsear(char *linea);
int conexionMemoria(int puerto, char*ip);
void apiKernel();
int mySelect(char * table, char *key);
int insert(char* table ,char* key ,char* value);
int create(char* table , char* consistency , char* numPart , char* timeComp);
int journal();
int describe(char *table);
int drop(char*table);
int add(char* memory , char* consistency);
void ejecutarScripts();
void inicializarColas();
FILE* avanzarLineas(int num,FILE * fp);
void inicializarListas();
struct memoria *asignarMemoriaSegunCriterio( char *consistency);
struct memoria *verMemoriaLibre(t_list *lista);
bool verificaMemoriaRepetida(u_int16_t id, t_list*criterio);
struct memoria * buscarMemoria(u_int16_t id);
void pruebas();
void mostrarResultados();
struct metadataTabla * buscarMetadataTabla(char* table);
void destruir();
void moverAcola(t_queue * a,t_queue *b);
int metrics(int modo);
void describeGlobal();
void limpiarMetadata();
void actualizarMetadataTabla(struct metadataTabla *m);
void metricasAutomaticas();

static sem_t semColasMutex;
static sem_t semColasContador;



#endif /*KERNEL_H_*/

