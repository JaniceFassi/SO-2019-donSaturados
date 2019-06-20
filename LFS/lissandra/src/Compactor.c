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
	while(cant>0){
		Tabla *dumpTabla=list_get(dump,cant-1);
		char **arrayBlock=malloc(sizeof(int));
		char *path=nivelUnaTabla(dumpTabla->nombre,0);
		if(folderExist(path)==0){
			//Calcular el tamaño de dumpT->registros
			char *buffer=largoDeRegistros(dumpTabla->registros);
			int largo=strlen(buffer)+1;
			//calcular cant bloques
			int bloquesNecesarios=largo/metaLFS->tamBloques;
			if(largo%metaLFS->tamBloques!=0){
				bloquesNecesarios++;
			}
			//int cantReg=list_size(dumpTabla->registros); 		se puede sacar?
			//pedir x cant bloques y guardarlas en un char           SI NO HAY ESPACIO Q PASA?  **PREGUNTAR**
			if(hayXBloquesLibres(bloquesNecesarios)){
				//COMPLETAR
				int i=0;
				while(i<bloquesNecesarios){
					arrayBlock[i]=string_itoa(obtenerBloqueVacio());
					i++;
				}
			}
			//Saca el numero de dump de esa tabla
			int cantTmp=contarTemporales(dumpTabla->nombre);
			char *ruta =nivelParticion(dumpTabla->nombre,cantTmp, 1);

			if(nuevoMetaArch(ruta, largo, arrayBlock,bloquesNecesarios)==1){
				log_info(logger,"ERROR AL CREAR LA METADATA DEL ARCHIVO\n");
				int i=0;
				while(arrayBlock[i]!=NULL){
					desocuparBloque(atoi(arrayBlock[i]));
					free(arrayBlock[i]);
					i++;
				}
				free(ruta);
			}
			escribirBloque(buffer,arrayBlock);

			list_destroy_and_destroy_elements(dumpTabla->registros,(void *)destroyRegistry);//ROMPE ACA POR UN FREE
			free(ruta);
		}
		liberarSubstrings(arrayBlock);
		free(path);
		free(dumpTabla->nombre);
		free(dumpTabla);
		cant--;
	}
	list_destroy(dump);
}
