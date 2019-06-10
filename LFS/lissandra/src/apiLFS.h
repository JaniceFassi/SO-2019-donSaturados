#ifndef APILFS_H_
#define APILFS_H_
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <string.h>
#include<commons/log.h>
#include<commons/string.h>
#include<commons/config.h>
#include<commons/txt.h>
#include<commons/collections/list.h>
#include "Lissandra.h"
#include "TADs.h"

/*typedef enum{
	SELECT,
	INSERT,
	CREATE,
	DESCRIBE,
	DROP
}op_code;*/

//FUNCIONES API
void drop(char* nameTable);
t_list *describe(char* nameTable,int variante);
int create(char* nameTable, char* consistency , u_int16_t numPartition,long timeCompaction);
char *selectS(char* nameTable, u_int16_t key);
int insert(char* nameTable, u_int16_t key, char *value, long timestamp);

#endif /* APILFS_H_ */
