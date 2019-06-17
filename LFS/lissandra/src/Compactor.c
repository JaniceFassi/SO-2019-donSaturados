/*
 * Compactor.c
 *
 *  Created on: 12 abr. 2019
 *      Author: utnso
 */

#include "Compactor.h"

void dump(){
	t_list *dump=list_duplicate(memtable);
	list_clean(memtable);
	int cant=list_size(dump);
	char *arrayBlock=malloc(5);
	while(cant>0){
		Tabla *dumpTabla=list_get(dump,cant-1);
		char *path=nivelUnaTabla(dumpTabla->nombre,0);
		if(folderExist(path)==0){
			//Calcular el tamaño de dumpT->registros
			int largo=largoDeRegistros(dumpTabla->registros);
			//calcular cant bloques
			int bloquesNecesarios=largo/metaLFS->tamBloques;
			if(largo%metaLFS->tamBloques!=0){
				bloquesNecesarios++;
			}
			//int cantReg=list_size(dumpTabla->registros); 		se puede sacar?
			//pedir x cant bloques y guardarlas en un char           SI NO HAY ESPACIO Q PASA?  **PREGUNTAR**
			if(hayXBloquesLibres(bloquesNecesarios)){
				//COMPLETAR
				char *block=string_itoa(obtenerBloqueVacio());
				strcpy(arrayBlock,block);
				arrayBlock=ponerSeparador(arrayBlock);
				if(bloquesNecesarios>1){
					for(int i =1;i<bloquesNecesarios;i++){
						block=string_itoa(obtenerBloqueVacio());
						arrayBlock=realloc(arrayBlock,strlen(block)+strlen(arrayBlock)+1);
						strcat(arrayBlock,block);
						arrayBlock=ponerSeparador(arrayBlock);
					}
				}
			}
			//Saca el numero de dump de esa tabla
			int cantTmp=contarTemporales(dumpTabla->nombre);
			char *ruta =nivelParticion(dumpTabla->nombre,cantTmp, 1);

			nuevoMetaArch(ruta, largo, arrayBlock);
			//escribirBloques();

			list_destroy_and_destroy_elements(dumpTabla->registros,(void *)destroyRegistry);			//ROMPE ACA POR UN FREE
			free(ruta);
		}
		free(path);
		free(dumpTabla->nombre);
		free(dumpTabla);
		cant--;
	}
	list_destroy(dump);
}
