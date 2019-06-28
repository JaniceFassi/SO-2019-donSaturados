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
#include "Lissandra.h"
#include "TADs.h"

int dump();
void compactar(char *nombreTabla);
#endif /* COMPACTOR_H_ */
