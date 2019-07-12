/*
 * Compactor.h
 *
 *  Created on: 13 abr. 2019
 *      Author: utnso
 */

#ifndef COMPACTOR_H_
#define COMPACTOR_H_
#include "TADs.h"
#include "apiLFS.h"
#include "FileSystem.h"
typedef struct{
	char *nombre;
	long time_compact;
	pthread_t hilo;
	sem_t *semaforoTMP;
	sem_t *semaforoTMPC;
	sem_t *semaforoBIN;
	sem_t *semaforoContarTMP;
	sem_t *borrarTabla;
	sem_t *semaforoMeta;
	int pedido_extension;//se inicializa en -1// si esta en 0 es BIN, si es TMP es 1 y 2 TMPC
}Sdirectorio;

t_list *directorioP;

//FUNCIONES SEMAFOROS
void semaforosTabla(Sdirectorio *nuevo);
void liberarSemaforosTabla(Sdirectorio *nuevo);


void compactar(Sdirectorio *nuevo);
int dump();
void levantarDirectorio();
void liberarDirectorio(Sdirectorio *nuevo);
void liberarDirectorioP();
Sdirectorio *obtenerUnaTabDirectorio(char *tabla);
//void compactar(char *nombreTabla,long tiempo_compactacion);
#endif /* COMPACTOR_H_ */
