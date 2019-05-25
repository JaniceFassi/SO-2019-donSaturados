/*
 * Lissandra.h
 *
 *  Created on: 9 abr. 2019
 *      Author: utnso
 */

#ifndef LISSANDRA_H_
#define LISSANDRA_H_

#include<stdio.h>
#include<stdlib.h>
#include <time.h>
#include <commons/temporal.h>
#include<commons/log.h>
#include<commons/string.h>
#include<commons/config.h>
#include <readline/readline.h>
#include <readline/history.h>
#include"apiLFS.h"
#include<commons/collections/node.h>
#include<commons/collections/list.h>
#include <socketSaturados.h>
#include "Compactor.h"

typedef enum{
	SELECT,
	INSERT,
	CREATE,
	DESCRIBE,
	DROP
}op_code;

t_log* logger;
t_config* config;
t_log* init_logger(void);
t_config* read_config(void);
t_list *memtable;

char *puntoMontaje;

void theStart();
void connectMemory(u_int16_t *cliente);
void console();
void dump();
void theEnd();
void exec_api(op_code mode, u_int16_t sock);
char* recibirDeMemoria(u_int16_t sock);

#endif /* LISSANDRA_H_ */
