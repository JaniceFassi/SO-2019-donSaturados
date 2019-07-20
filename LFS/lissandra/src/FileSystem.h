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
#include "TADs.h"
#include <dirent.h>

typedef struct{
	char *nombreTabla;
	int extension;//0 bin 1 temp  2tmpC
	int contador;
}archAbierto;

t_list *tablaArchGlobal;
//FUNCIONES DE DIRECTORIO
int crearMontaje();
int crearNivelMetadata();
int crearNivelTablas();
int crearNivelBloques();
//FUNCIONES DE ABRIR Y CERRAR ARCHIVOS
bool archivoYaAbierto(char *tabla,int extension);
archAbierto *obtenerArch(char *tabla, int extension);
void nuevoArch(char *tabla, int extension);
void liberarArch(archAbierto *nuevo);
void sacarArch(char *tabla,int extension);
#endif /* FILESYSTEM_H_ */
