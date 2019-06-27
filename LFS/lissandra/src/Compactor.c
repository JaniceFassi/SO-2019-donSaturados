/*
 * Compactor.c
 *
 *  Created on: 12 abr. 2019
 *      Author: utnso
 */

#include "Compactor.h"
int dump(){
	log_info(logger,"dump");
	t_list *dump=list_duplicate(memtable);
	list_clean(memtable);
	int cant=list_size(dump);
	while(cant>0){
		Tabla *dumpTabla=list_get(dump,cant-1);
		char *path=nivelUnaTabla(dumpTabla->nombre,0);
		if(folderExist(path)==0){
			//Calcular el tamaño de dumpTabla->registros
			int cantTmp=contarArchivos(dumpTabla->nombre, 1);
			char *ruta =nivelParticion(dumpTabla->nombre,cantTmp, 1);
			//Registry *reg=list_get(dumpTabla->registros,0);
			//log_info(logger,"%s",reg->value);
			if(escribirParticion(ruta,dumpTabla->registros,0)==1){
				log_error(logger,"error al escribir el dump");
				free(ruta);
				free(path);
				list_destroy(dump);//_and_destroy_elements(dump,(void *)liberarTabla);
				return 1;
			}
			free(ruta);
		}
		//liberarTabla(dumpTabla);
		free(path);
		cant--;
	}
	list_destroy(dump);
	return 0;
}

void compactar(char *nombreTabla){
	log_info(logger,"Se empezo a hacer la compactacion de la tabla %s",nombreTabla);
	char *path=nivelUnaTabla(nombreTabla,0);
	if(folderExist(path)==1){
		log_error(logger,"No se puede hacer la compactacion por que la tabla %s ya no existe",nombreTabla);
		free(path);
		return;
	}
	free(path);
	int cantTemp=contarArchivos(nombreTabla,1);
	//cambiar nombre de temp a tempc
	if(cantTemp==0){
		log_info(logger,"No hay datos para la compactacion de %s",nombreTabla);
		return;
	}
	int contador=0;
	while(cantTemp>contador){
		char *arch=nivelParticion(nombreTabla,contador,1);
		//semaforos
		if(renombrarTemp_TempC(arch)==1){
			log_error(logger,"No se puede hacer la compactacion por que no se pudo renombrar");
			free(arch);
			return;
		}
		free(arch);
		contador++;
	}
	//sacar la metadata
	metaTabla *metadata=leerMetadataTabla(nombreTabla);
	t_list *todosLosRegistros=list_create();
	contador=0;
	int cantMax=metadata->partitions;
	int binario=0;
	//leer Arch
	while(cantMax>contador){
		char *arch;
		if(binario==0){
			//leer binarios
			arch=nivelParticion(nombreTabla,contador,0);
			if(contador+1==cantMax){
				contador=-1;
				cantMax=cantTemp;
				binario++;
			}
		}else{
			//leerTempc
			arch=nivelParticion(nombreTabla,contador,2);
		}
		escanearArchivo(arch,todosLosRegistros);
		liberarParticion(arch);
		free(arch);
		contador++;
	}
	contador=0;
	t_list *depurado=regDep(todosLosRegistros);
	//escribir
	while(contador<metadata->partitions){
		t_list *particion=filtrarPorParticion(depurado,contador,metadata->partitions);
		char *archP=nivelParticion(nombreTabla,contador,0);
		if(escribirParticion(archP,particion,1)==1){
			log_error(logger,"Error a la hora de escribir la compactacion");
			free(archP);
			list_destroy(particion);
			borrarMetadataTabla(metadata);
			list_destroy(depurado);
			list_destroy_and_destroy_elements(todosLosRegistros,(void *)destroyRegistry);
			return;
		}
		free(archP);
		list_destroy(particion);
		contador++;
	}
	list_destroy(depurado);
	borrarMetadataTabla(metadata);
	list_destroy_and_destroy_elements(todosLosRegistros,(void *)destroyRegistry);
	log_info(logger,"Fin de la compactacion de %s",nombreTabla);
	//aca deberia activar el tiempo de compactacion de nuevo
}
