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

typedef struct{
	char *name;
	u_int16_t key;
	char *value;
	long timestamp;

}Registry;

typedef struct{
	char *consistency;
	int partitions;
	long compaction_time;
}metaTabla;

//FUNCIONES DE TAD REGISTRY

void destroyRegistry(Registry *self);
Registry *createRegistry(char *table, u_int16_t key, char *val, long time);
Registry *getList();

//FUNCIONES DE CARPETAS Y ARCHIVOS
char *pathFinal(char *nombre, int principal,char *path);
int crearCarpeta(char* path);
int folderExist(char* path);
int borrarCarpeta(char *path);
int crearParticiones(char *nombre, int cantidad);
void crearArchMetadata(char* path, char* consistency , u_int16_t numPartition,long timeCompaction);
metaTabla *leerArchMetadata(char *path);

#endif /* LFS_LISSANDRA_SRC_FILESYSTEM_H_ */
