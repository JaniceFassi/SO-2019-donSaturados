/*
 * Compactor.h
 *
 *  Created on: 13 abr. 2019
 *      Author: utnso
 */

#ifndef COMPACTOR_H_
#define COMPACTOR_H_
#include <stdio.h>
#include <stdlib.h>
#include<commons/log.h>
#include<commons/string.h>
#include<commons/config.h>
#include<readline/readline.h>
#include "TADs.h"
#include "Lissandra.h"
typedef struct{
	char *nombre;
	long time_compact;
	pthread_t hilo;
}testT;
void liberarTest(testT *nuevo);

int dump();
void compactar(testT *nuevo);
//void compactar(char *nombreTabla,long tiempo_compactacion);
#endif /* COMPACTOR_H_ */
