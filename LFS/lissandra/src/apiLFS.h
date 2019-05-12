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
#include "Lissandra.h"

typedef enum{
	SELECT,
	INSERT,
	CREATE,
	DESCRIBE,
	DROP
}op_code;

//FUNCIONES API
void drop(char* nameTable);
int describe(char* nameTable, t_list *tablas,int variante);
void create(char* nameTable, char* consistency , u_int16_t numPartition,long timeCompaction);
int selectS(char* nameTable, u_int16_t key, char* valor);
void insert(char* nameTable, u_int16_t key, char *value, long timestamp);

#endif /* APILFS_H_ */
