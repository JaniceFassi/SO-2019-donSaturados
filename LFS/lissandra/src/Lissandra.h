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

typedef enum{
	SELECT,
	INSERT,
	CREATE,
	DESCRIBE,
	DROP
}op_code;

t_log* init_logger(void);
t_config* init_config(void);
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
