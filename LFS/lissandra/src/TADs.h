#ifndef TADS_H
#define TADS_H

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/stat.h>
#include<commons/log.h>
#include <fcntl.h>
#include <sys/types.h>
#include<commons/string.h>
#include<commons/config.h>
#include <commons/bitarray.h>
#include <sys/mman.h>
#include<readline/readline.h>
#include "Lissandra.h"

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

t_bitarray* bitmap;
int archivoBitmap;
datosConfig *configLissandra;
metaFileSystem *metaLFS;
t_list *directorio;
/*char *pathConfig;
*/
//FUNCIONES DE REGISTROS
void destroyRegistry(Registry *self);
Registry *createRegistry(u_int16_t key, char *val, long time);
Registry *getList();
void agregarRegistro(Tabla *name,u_int16_t key, char *val, long time);
Registry *encontrarKeyDepu(t_list *registros,int key);
Registry *keyConMayorTime(t_list *registros);
int encontrarRegistroPorKey(t_list *registros,int key);
t_list* filtrearPorKey(t_list *registros,int key);
int calcularIndex(t_list *lista,int key);

void oldCrearMetaLFS(u_int16_t size,u_int16_t cantBloques,char *magicNumber);
void crearMetaLFS();
char *nivelTablas();
char *nivelBloques();
char *nivelParticion(char *tabla, int particion, int modo);
void leerMetaLFS();
char *rutaBloqueNro(int nroBloque);
void borrarMetaLFS();
char *nivelUnaTabla(char *nombre, int modo);
void estructurarConfig();
void borrarDatosConfig();
void crearConfig();

//FUNCIONES DE TABLAS
Tabla *crearTabla(char *nombre,u_int16_t key, char *val, long time);
Tabla *find_tabla_by_name(char *name);
void liberarTabla(Tabla *self);
t_list *regDep(t_list *aDepu);

//FUNCIONES DE CONCATENAR
char *pathFinal(char *nombre, int principal);
char *concatParaArchivo(long timestamp,int key,char *value,int opc);
Registry *desconcatParaArch(char *linea);
char *concatExtencion(char *name,int particion, int tipo);
char *extension(char *path,int modo);
char *ponerBarra(char *linea);
char *obtenerMontaje();
char *nivelMetadata(int modo);
char *concatRegistro(Registry *reg);
char *ponerSeparador(char *linea);
char *array_A_String(char **array,int cantBloques);
void liberarSubstrings(char **liberar);
//FUNCIONES DE CARPETAS
int crearCarpeta(char* path);
int folderExist(char* path);
int borrarCarpeta(char *path);

//FUNCIONES ARCHIVOS
int crearParticiones(metaTabla *tabla);
metaTabla *crearMetadataTabla(char* nombre, char* consistency , u_int16_t numPartition,long timeCompaction);
metaTabla *leerMetadataTabla(char *nombre);
void borrarMetadataTabla(metaTabla *metadata);
int escribirArchBinario(char *path,long timestamp,int key,char *value);
t_list *leerTodoArchBinario(char *path);
int agregarArchBinario(char *path,long timestamp,int key,char *value);
int eliminarArchivo(char *path);
void escribirReg(char *name,t_list *registros,int cantParticiones);
int archivoValido(char *path);
//void crearMetaArchivo(char *path, int nrobloque);			ya se puede sacar
int crearMetaArchivo(char *path, int size, char **bloques, int cantBloques);
void borrarMetaArch(metaArch *nuevo);
int tamanioArchivo(char* path);
int contarTemporales(char *nombre);
metaArch *leerMetaArch(char *path);
//NUEVAS FUNCIONES ARCHIVOS
void escribirBloque(char *buffer,char **bloques);
void escribirArchB(char *path,char *buffer);
t_list *deChar_Registros(char *buffer);
char *leerArchBinario(char *path,int tamanio);
t_list *leerBloques(char**bloques,int offset);
//FUNCIONES BITMAPS
void cargarBitmap();
void mostrarBitmap();
char *inicializarArray();
bool hayXBloquesLibres(int cantidad);
int cantBloquesLibres(int cantidad);
int obtenerBloqueVacio();
void desocuparBloque(int Nrobloque);
void ocuparBloque(int Nrobloque);
char *cadenaDeRegistros(t_list *lista);

#endif
