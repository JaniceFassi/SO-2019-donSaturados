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

void api(op_code option);
void api(op_code option);
void drop(char* nameTable);
t_list* describe();
t_list* describe2(char* nameTable);
void create(char* nameTable, char* consistency , u_int16_t numPartition,long timeCompaction);
char* selects(char* nameTable , u_int16_t key);//porque rompe con select?? PREGUNTA
void insert(char* nameTable, u_int16_t key, char *value);
void insert2(char* nameTable, u_int16_t key, char *value, long timestamp);


#endif /* APILFS_H_ */
