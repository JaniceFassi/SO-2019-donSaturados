/*
 * Compactor.c
 *
 *  Created on: 12 abr. 2019
 *      Author: utnso
 */

#include "Compactor.h"

int dump(){
	t_list *dump=list_duplicate(memtable);
	list_clean(memtable);
	int cant=list_size(dump);
	char ** arrayBlock;
	int bloquesNecesarios=0;
	int largo;
	while(cant>0){
		Tabla *dumpTabla=list_get(dump,cant-1);
		largo=0;
		char *path=nivelUnaTabla(dumpTabla->nombre,0);
		if(folderExist(path)==0){
			//Calcular el tamaño de dumpTabla->registros
			char *buffer=cadenaDeRegistros(dumpTabla->registros);
			largo=strlen(buffer)+1;
			//calcular cant bloques
			bloquesNecesarios=largo/metaLFS->tamBloques;
			if(largo%metaLFS->tamBloques!=0){
				bloquesNecesarios++;
			}
			arrayBlock=malloc(sizeof(int)*bloquesNecesarios);

			//pedir x cant bloques y guardarlas en un char
			if(!hayXBloquesLibres(bloquesNecesarios)){
				//COMPLETAR
				log_info(logger,"No hay suficientes bloques como para realizar el dump, se perderán los datos de la memtable.");
				return 1;
			}
			int i=0;
			while(i<bloquesNecesarios){
				int bloqueVacio=obtenerBloqueVacio();
				arrayBlock[i]=string_itoa(bloqueVacio);
				i++;
			}

			//Saca el numero de dump de esa tabla
			int cantTmp=contarArchivos(dumpTabla->nombre, 1);
			char *ruta =nivelParticion(dumpTabla->nombre,cantTmp, 1);
			if(crearMetaArchivo(ruta, largo, arrayBlock,bloquesNecesarios)==1){
				log_info(logger,"ERROR AL CREAR LA METADATA DEL ARCHIVO.\n");
				int i=0;
				while(arrayBlock[i]!=NULL){
					desocuparBloque(atoi(arrayBlock[i]));
					free(arrayBlock[i]);
					i++;
				}
				free(ruta);
			}
			escribirBloque(buffer,arrayBlock);

			list_destroy_and_destroy_elements(dumpTabla->registros,(void *)destroyRegistry);
			free(ruta);
		}
		for(int f=0;f<bloquesNecesarios;f++){
			free(arrayBlock[f]);
		}
		free(arrayBlock);
		free(path);
		free(dumpTabla->nombre);
		free(dumpTabla);
		cant--;
	}
	list_destroy(dump);
	return 0;
}
