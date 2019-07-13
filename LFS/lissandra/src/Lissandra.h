/*
 * Lissandra.h
 *
 *  Created on: 9 abr. 2019
 *      Author: utnso
 */

#ifndef LISSANDRA_H_
#define LISSANDRA_H_

#include "Compactor.h"
#include "FileSystem.h"
#include"apiLFS.h"
#include <dirent.h>
#include <sys/inotify.h>

typedef enum{
	SELECT,
	INSERT,
	CREATE,
	DESCRIBE,
	DROP,
	EXIT,
	EXTRA,
	OTRO,
	HANDSHAKE
}op_code;

pthread_t hiloMemoria;
t_log* init_logger(void);
t_config* init_config(void);
void theStart();
void funcionSenial(int sig);
void *connectMemory(u_int16_t *server);
void *interactuarConMemoria(u_int16_t *arg);
char* recibirDeMemoria(u_int16_t sock);
void *inicializarInotify();
void mostrarDescribe(t_list *lista);
char *empaquetarDescribe(t_list *lista);
void exec_api(op_code mode, u_int16_t sock);
void *console();
void theEnd();



#endif /* LISSANDRA_H_ */
