/*
 * FileSystemN.h
 *
 *  Created on: 12 abr. 2019
 *      Author: utnso
 */

#ifndef FILESYSTEM_H_
#define FILESYSTEM_H_

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/stat.h>
#include<commons/log.h>
#include <fcntl.h>
#include<commons/string.h>
#include<commons/config.h>
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

//FUNCIONES DE CARPETAS
int crearCarpeta(char* path);
int folderExist(char* path);
int borrarCarpeta(char *path);

//FUNCIONES ARCHIVOS
int crearParticiones(char *nombre, int cantidad);
void crearArchMetadata(char* nombre, char* consistency , u_int16_t numPartition,long timeCompaction);
metaTabla *leerArchMetadata(char *nombre);
int escribirArchBinario(char *path,long timestamp,int key,char *value);
t_list *leerTodoArchBinario(char *path);
int agregarArchBinario(char *path,long timestamp,int key,char *value);
int eliminarArchivo(char *path);
void escribirReg(char *name,t_list *registros,int cantParticiones);
int archivoValido(char *path);


#endif /* LFS_LISSANDRA_SRC_FILESYSTEM_H_ */
