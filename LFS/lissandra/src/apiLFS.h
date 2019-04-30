/*
 * api.h
 *
 *  Created on: 9 abr. 2019
 *      Author: utnso
 */

#ifndef APILFS_H_
#define APILFS_H_
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <string.h>
#include<commons/log.h>
#include<commons/string.h>
#include<commons/config.h>
#include<commons/collections/list.h>
#include "FileSystem.h"

typedef enum{
	SELECT,
	INSERT,
	CREATE,
	DESCRIBE,
	DROP
}op_code;

typedef struct{
	char *name;
	u_int16_t key;
	char *value;
	long timestamp;

}Registry;

void api(op_code option);

//FUNCIONES API
void drop(char* nameTable);
t_list* describe(char* nameTable);
void create(char* nameTable, char* consistency , u_int16_t numPartition,long timeCompaction);
char* selectS(char* nameTable, u_int16_t key);
void insert(char* nameTable, u_int16_t key, char *value, long timestamp);

//FUNCIONES DE TAD REGISTRY
void destroyRegistry(Registry *self);
Registry *createRegistry(char *table, u_int16_t key, char *val, long time);
Registry *getList();
#endif /* APILFS_H_ */
