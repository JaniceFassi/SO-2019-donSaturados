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
int drop(char* nameTable);
t_list *describe(char* nameTable);
int create(char* nameTable, char* consistency , u_int16_t numPartition,long timeCompaction);
int insert(char* nameTable, u_int16_t key, char *value, long timestamp);
char *lSelect(char *nameTable, u_int16_t key);

#endif /* APILFS_H_ */
