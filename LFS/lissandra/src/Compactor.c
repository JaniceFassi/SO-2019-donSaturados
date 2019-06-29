/*
 * Compactor.c
 *
 *  Created on: 12 abr. 2019
 *      Author: utnso
 */

#include "Compactor.h"
int dump(){

	//ACA IRIA EL WAIT MUTEX DE LA MEMTABLE
	t_list *dump=list_duplicate(memtable);
	list_clean(memtable);
	//ACA IRIA EL SIGNAL MUTEX DE LA MEMTABLE

	int cant=list_size(dump);
	while(cant>0){
		Tabla *dumpTabla=list_get(dump,cant-1);
		char *path=nivelUnaTabla(dumpTabla->nombre,0);
		if(folderExist(path)==0){
			int cantTmp=contarArchivos(dumpTabla->nombre, 1);
			char *ruta =nivelParticion(dumpTabla->nombre,cantTmp, 1);

			if(escribirParticion(ruta,dumpTabla->registros,0)==1){
				log_error(logger,"error al escribir el dump");
				free(ruta);
				free(path);
				list_destroy_and_destroy_elements(dump,(void *)liberarTabla);
				return 1;
			}
			free(ruta);
		}
		liberarTabla(dumpTabla);
		free(path);
		cant--;
	}
	list_destroy(dump);
	return 0;
}

void liberarTest(testT *nuevo){
	free(nuevo->nombre);
	pthread_kill(nuevo->hilo,0);
	free(nuevo);
}

//void compactar(char *nombreTabla, long tiempoCompactacion){
void compactar(testT* nuevo){
while(1){
	sleep(nuevo->time_compact);
	log_info(logger,"Se empezo a hacer la compactacion de la tabla %s",nuevo->nombre);
	char *path=nivelUnaTabla(nuevo->nombre,0);
	if(folderExist(path)==1){
		log_error(logger,"No se puede hacer la compactacion por que la tabla %s ya no existe",nuevo->nombre);
		free(path);
		return;
	}
	free(path);
	int cantTemp=contarArchivos(nuevo->nombre,1);
	//Cambiar nombre de tmp a tmpc
	if(cantTemp==0){
		log_info(logger,"No hay datos para la compactacion de %s.",nuevo->nombre);
		return;
	}
	int contador=0;
	while(cantTemp>contador){
		char *arch=nivelParticion(nuevo->nombre,contador,1);
		//semaforos
		if(renombrarTemp_TempC(arch)==1){
			log_error(logger,"No se puede hacer la compactacion porque no se pudo renombrar los temporales.");
			free(arch);
			return;
		}
		free(arch);
		contador++;
	}
	//sacar la metadata
	metaTabla *metadata=leerMetadataTabla(nuevo->nombre);
	t_list *todosLosRegistros=list_create();
	contador=0;
	int cantMax=metadata->partitions;
	int binario=0;
	//leer Arch
	while(cantMax>contador){
		char *arch;
		if(binario==0){
			//leer binarios
			arch=nivelParticion(nuevo->nombre,contador,0);
			if(contador+1==cantMax){
				contador=-1;
				cantMax=cantTemp;
				binario++;
			}
		}else{
			//leerTempc
			arch=nivelParticion(nuevo->nombre,contador,2);
		}
		escanearArchivo(arch,todosLosRegistros);
		free(arch);
		contador++;
	}
	contador=0;
	t_list *depurado=regDep(todosLosRegistros);
	//escribir
	//WAIT MUTEX DE BINARIOS DE LA TABLA
	while(contador<metadata->partitions){
		t_list *particion=filtrarPorParticion(depurado,contador,metadata->partitions);
		char *archP=nivelParticion(nuevo->nombre,contador,0);
		liberarParticion(archP);
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
	}//SIGNAL MUTEX BINARIOS DE LA TABLA
	//liberar TMPC
	contador=0;
	//WAIT MUTEX TMPC DE LA TABLA
	while(cantTemp>contador){
		char *arch=nivelParticion(nuevo->nombre,contador,2);
		liberarParticion(arch);
		free(arch);
		contador++;
	}
	//SIGNAL MUTEX TMPC DE LA TABLA
	list_destroy(depurado);
	borrarMetadataTabla(metadata);
	list_destroy_and_destroy_elements(todosLosRegistros,(void *)destroyRegistry);
	log_info(logger,"Fin de la compactacion de %s",nuevo->nombre);
	//aca deberia activar el tiempo de compactacion de nuevo
}
}
