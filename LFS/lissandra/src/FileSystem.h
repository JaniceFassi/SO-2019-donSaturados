/*
 * FileSystem.h
 *
 *  Created on: 9 jun. 2019
 *      Author: utnso
 */

#ifndef FILESYSTEM_H_
#define FILESYSTEM_H_

#include<stdio.h>
#include<stdlib.h>
#include <time.h>
#include <commons/temporal.h>
#include<commons/log.h>
#include<commons/string.h>
#include<commons/config.h>
#include <readline/readline.h>
#include <readline/history.h>
#include<commons/collections/node.h>
#include<commons/collections/list.h>
#include <sys/types.h>
#include <signal.h>
#include <socketSaturados.h>
#include"apiLFS.h"
#include "Compactor.h"
#include "FileSystem.h"
#include "TADs.h"

int crearMontaje();
int crearNivelMetadata();
int crearNivelTablas();
int crearNivelBloques();
void levantarDirectorio();

#endif /* FILESYSTEM_H_ */
