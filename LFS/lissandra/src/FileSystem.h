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
#include <sys/stat.h>
#include<commons/log.h>
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

//FUNCIONES DE TAD REGISTRY
void destroyRegistry(Registry *self);
Registry *createRegistry(char *table, u_int16_t key, char *val, long time);
Registry *getList();
//FUNCIONES DE CARPETAS Y ARCHIVOS
int folderExist(char* name);
int carpTabla(char* puntoMontaje);
#endif /* LFS_LISSANDRA_SRC_FILESYSTEM_H_ */
