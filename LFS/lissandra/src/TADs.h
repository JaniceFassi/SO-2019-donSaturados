#ifndef TADS_H
#define TADS_H

#include<stdio.h>
#include<stdlib.h>
#include <unistd.h>
#include <time.h>
#include <commons/temporal.h>
#include<commons/log.h>
#include<commons/string.h>
#include<commons/config.h>
#include <readline/readline.h>
#include <readline/history.h>
#include<commons/collections/node.h>
#include<commons/collections/list.h>
#include <sys/types.h>
#include <signal.h>
#include <socketSaturados.h>
#include <pthread.h>
#include <sys/inotify.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <sys/types.h>
#include <commons/bitarray.h>
#include <semaphore.h>
#include <sys/mman.h>
#include "Compactor.h"

typedef struct {
	u_int16_t key;
	char *value;
	long timestamp;
}Registry;

typedef struct{
	char *nombre;
	t_list *registros;
}Tabla;

typedef struct{
	char *nombre;
	char *consistency;
	int partitions;
	long compaction_time;
}metaTabla;

typedef struct{
	u_int16_t tamBloques;
	u_int16_t cantBloques;
	char *magicNumber;
}metaFileSystem;

typedef struct{
	u_int16_t size;
	char **bloques;
}metaArch;

typedef struct{
	char *puntoMontaje;
	u_int16_t tamValue;
	int id;
	int idEsperado;
	char *Ip;
	u_int16_t puerto;
	u_int16_t tiempoDump;
	u_int16_t retardo;
}datosConfig;

t_log* logger;
t_list *memtable;
t_bitarray* bitmap;
int archivoBitmap;
datosConfig *configLissandra;
metaFileSystem *metaLFS;
int cantBloqGlobal;
char *pathInicial;
char *raizDirectorio;

//semaforos
sem_t *criticaMemtable;
sem_t *criticaDirectorio;
sem_t *criticaTablaGlobal;
sem_t *criticaCantBloques;
sem_t *criticaBitmap;


//FUNCIONES DE TABLA DE ARCHIVOS GLOBAL
metaArch *abrirArchivo(char *tabla,int nombre,int extension);
void cerrarArchivo(char *tabla,int extension, metaArch *arch);
void cerrarMetaTabGlobal(char *tabla,t_config *arch);
t_config *abrirMetaTabGlobal(char *tabla);

//FUNCIONES SEMAFOROS
void inicializarSemGlob();
void liberarSemaforos();
//FUNCIONES DE REGISTROS
Registry *createRegistry(u_int16_t key, char *val, long time);
void agregarRegistro(Tabla *name,u_int16_t key, char *val, long time);
Registry *primerRegistroConKey(t_list *registros,int key);
Registry *regConMayorTime(t_list *registros);
int existeKeyEnRegistros(t_list *registros,int key);
t_list* filtrearPorKey(t_list *registros,int key);
int calcularIndexReg(t_list *lista,int key);
t_list *filtrarPorParticion(t_list *lista,int particion,int cantPart);//NUEVA FUNCION

//FUNCIONES DE TABLAS
Tabla *crearTabla(char *nombre,u_int16_t key, char *val, long time);
Tabla *find_tabla_by_name_in(char *name, t_list *l);
t_list *regDep(t_list *aDepu);
int calcularIndexTab(Tabla *t,t_list *l);
int calcularIndexTabPorNombre(char *nombre, t_list *lista);

//FUNCIONES DE CONCATENAR
char *extension(char *path,int modo);
char *ponerBarra(char *linea);
char *nivelTablas();
char *nivelBloques();
char *rutaBloqueNro(int nroBloque);
char *nivelParticion(char *tabla, int particion, int modo);
char *obtenerMontaje();
char *nivelUnaTabla(char *nombre, int modo);
char *nivelMetadata(int modo);
char *concatRegistro(Registry *reg);
char *ponerSeparador(char *linea);
char *array_A_String(char **array,int cantBloques);
char *cadenaDeRegistros(t_list *lista);
int calcularIndexName(char *name);
//FUNCIONES QUE DESCONCATENAN
t_list *deChar_Registros(char *buffer);
Registry *desconcatParaArch(char *linea);

//FUNCIONES DE CARPETAS
int crearCarpeta(char* path);
int folderExist(char* path);
int borrarCarpeta(char *path);

//FUNCIONES ARCHIVOS
int crearParticiones(metaTabla *tabla);
metaTabla *crearMetadataTabla(char* nombre, char* consistency , u_int16_t numPartition,long timeCompaction);
metaTabla *leerMetadataTabla(char *nombre);
metaTabla *levantarMetadataTabla(char *nombre);
metaTabla *obtenerMetadataTabla(char *nombre, t_config *arch);
int archivoValido(char *path);
void escanearArchivo(char *nameTable,int part,int extension, t_list *obtenidos);
int crearMetaArchivo(char *path, int size, char **bloques, int cantBloques);
int contarArchivos(char *nombre, int modo);
void crearConfig();
metaArch *leerMetaArch(char *path);
void escribirArchB(char *path,char *buffer);
char *leerArchBinario(char *path,int tamanio);
void oldCrearMetaLFS(u_int16_t size,u_int16_t cantBloques,char *magicNumber);
void crearMetaLFS();
void leerMetaLFS();
void estructurarConfig();
int renombrarTemp_TempC(char *path);//NUEVA FUNCION
int escribirParticion(char *path,t_list *lista,int modo);//NUEVA FUNCION

//FUNCIONES DE BLOQUES
bool hayXBloquesLibres(int cantidad);
int cantBloquesLibres(int cantidad);
int obtenerBloqueVacio();
void desocuparBloque(int Nrobloque);
void ocuparBloque(int Nrobloque);
void escribirBloque(char *buffer,char **bloques);
t_list *leerBloques(char**bloques,int offset);
int pedirBloques(int cantidad, char **array);//NUEVO

//FUNCIONES BITMAPS
void cargarBitmap();
void mostrarBitmap();

//FUNCIONES QUE LIBERAN MEMORIA
void borrarMetaArch(metaArch *nuevo);
int eliminarArchivo(char *path);
void liberarSubstrings(char **liberar);
void borrarMetadataTabla(metaTabla *metadata);
void borrarDatosConfig();
void borrarMetaLFS();
void liberarTabla(Tabla *self);
void destroyRegistry(Registry *self);
void liberarParticion(char *path);//NUEVO
void limpiarBloque(char* nroBloque);//NUEVO
void limpiarArchivo(char* pathArchivo);//NUEVO
void liberarArraydeBloques(char **array);//NUEVO

#endif
