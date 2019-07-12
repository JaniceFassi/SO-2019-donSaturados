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
#include <sys/types.h>
#include <signal.h>
#include <socketSaturados.h>
#include "Compactor.h"
#include "FileSystem.h"
#include "TADs.h"
#include <pthread.h>
#include <sys/inotify.h>

typedef enum{
	SELECT,
	INSERT,
	CREATE,
	DESCRIBE,
	DROP
}op_code;

t_log* logger;
t_log* init_logger(void);
t_config* init_config(void);
t_list *memtable;
char *pathInicial;
char *raizDirectorio;
void theStart();
void *inicializarInotify();
void *connectMemory(u_int16_t *server);
void *interactuarConMemoria(u_int16_t *arg);
void *console();
void funcionSenial(int sig);
void theEnd();
void exec_api(op_code mode, u_int16_t sock);
char* recibirDeMemoria(u_int16_t sock);

#endif /* LISSANDRA_H_ */
