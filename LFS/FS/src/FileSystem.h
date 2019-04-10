/*
 * FileSystem.h
 *
 *  Created on: 9 abr. 2019
 *      Author: utnso
 */

#ifndef FILESYSTEM_H_
#define FILESYSTEM_H_
#include <stdio.h>
#include <stdlib.h>
#include<commons/log.h>
#include<commons/string.h>
#include<commons/config.h>
#include<readline/readline.h>

t_log* init_logger(void);
t_config* read_config(void);

void insert(long timestamp, u_int16_t key, char *value);

#endif /* FILESYSTEM_H_ */


