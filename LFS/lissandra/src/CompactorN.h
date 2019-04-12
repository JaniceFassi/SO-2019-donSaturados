/*
 * CompactorN.h
 *
 *  Created on: 12 abr. 2019
 *      Author: utnso
 */

#ifndef LFS_LISSANDRA_SRC_COMPACTORN_H_
#define LFS_LISSANDRA_SRC_COMPACTORN_H_

#include <stdio.h>
#include <stdlib.h>
#include<commons/log.h>
#include<commons/string.h>
#include<commons/config.h>
#include<readline/readline.h>
t_log* init_logger(void);
t_config* read_config(void);

#endif /* LFS_LISSANDRA_SRC_COMPACTORN_H_ */
