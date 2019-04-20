/*
 * Lissandra.h
 *
 *  Created on: 9 abr. 2019
 *      Author: utnso
 */

#ifndef LISSANDRA_H_
#define LISSANDRA_H_

#include <stdio.h>
#include <stdlib.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <socketSaturados.h>
#include<commons/log.h>
#include<commons/string.h>
#include<commons/config.h>
#include<readline/readline.h>
#include "apiLFS.h"
#include<commons/collections/node.h>
#include<commons/collections/list.h>

#include "Compactor.h"

t_log* init_logger(void);
t_config* read_config(void);

typedef struct{
	long timestamp;
	u_int16_t key;
	char* value;
}Registry;


//NO ESTA COMPLETAMENTE DECIDIDO

#endif /* LISSANDRA_H_ */
