/*
 * Compactor.c
 *
 *  Created on: 12 abr. 2019
 *      Author: utnso
 */

#include "Compactor.h"
void *dump(){
	//ACA IRIA EL WAIT MUTEX DE LA MEMTABLE
	while(1){
		usleep(configLissandra->tiempoDump*1000);
		sem_wait(sem_dump);
		//if(abortar==0){
			//pthread_exit(NULL);
		//}
		sem_wait(criticaMemtable);
		if(list_is_empty(memtable)){
			log_info(dumplog,"la memtable esta vacia por lo cual no se hace el dump");
			sem_post(criticaMemtable);
		}else{
			t_list *dump=list_duplicate(memtable);
			list_clean(memtable);
			//ACA IRIA EL SIGNAL MUTEX DE LA MEMTABLE
			sem_post(criticaMemtable);
			int cant=list_size(dump);
			while(cant>0){
				log_info(dumplog,"se empezo a hacer el dump");
				Tabla *dumpTabla=list_get(dump,cant-1);
				char *path=nivelUnaTabla(dumpTabla->nombre,0);
				if(folderExist(path)==0){
					log_info(dumplog,"se empezo a hacer el dump de %s",dumpTabla->nombre);
					Sdirectorio *tabDirectorio=obtenerUnaTabDirectorio(dumpTabla->nombre);
					sem_wait(&tabDirectorio->semaforoContarTMP);
					int cantTmp=contarArchivos(dumpTabla->nombre, 1);
					char *ruta =nivelParticion(dumpTabla->nombre,cantTmp, 1);

					if(escribirParticion(ruta,dumpTabla->registros,0)==1){
						log_error(logger,"error al escribir el dump");
					}
					sem_post(&tabDirectorio->semaforoContarTMP);
					free(ruta);
				}
				liberarTabla(dumpTabla);
				free(path);
				cant--;
				log_info(dumplog,"se termino de hacer el dump");
			}
			list_destroy(dump);
		}
		sem_post(sem_dump);
	}
	//return 0;
	pthread_exit(NULL);
}
//void compactar(char *nombreTabla, long tiempoCompactacion){
void compactar(Sdirectorio* nuevo){
	int seguir=1;
	while(seguir){
		usleep(nuevo->time_compact*1000);
		sem_getvalue(&nuevo->borrarTabla,&seguir);
		if(seguir==0){
			log_info(compaclog,"No se puede hacer la compactacion de la tabla %s porque hay pedido de borrar",nuevo->nombre);
			pthread_exit(NULL);
		}
		nuevo->pedido_extension=3;
		log_info(compaclog,"Se empezo a hacer la compactacion de la tabla %s",nuevo->nombre);
		char *path=nivelUnaTabla(nuevo->nombre,0);
		if(folderExist(path)==1){
			log_error(compaclog,"No se puede hacer la compactacion por que la tabla %s ya no existe",nuevo->nombre);
			free(path);
			pthread_exit(NULL);
		}
		free(path);
		sem_wait(&nuevo->semaforoContarTMP);
		int cantTemp=contarArchivos(nuevo->nombre,1);
		//Cambiar nombre de tmp a tmpc
		if(cantTemp==0){
			log_info(compaclog,"No hay datos para la compactacion de %s.",nuevo->nombre);
			sem_post(&nuevo->semaforoContarTMP);
			compactar(nuevo);
		}else{
			int contador=0;
			// SEMAFORO WAIT PEDIDO RENOMBRAR
			nuevo->pedido_extension=1;
			sem_wait(&nuevo->semaforoTMP);
			while(cantTemp>contador){
				char *arch=nivelParticion(nuevo->nombre,contador,1);
				//semaforos
				if(renombrarTemp_TempC(arch)==1){
					log_error(compaclog,"No se puedo renombrar el tmp de la tabla %s.",nuevo->nombre);
				}
				free(arch);
				contador++;
			}
			log_info(compaclog,"Se pudieron renombrar los tmps de la tabla %s.",nuevo->nombre);
			sem_post(&nuevo->semaforoContarTMP);
			//SIGNAL WAIT PEDIDO RENOMBRAR
			sem_post(&nuevo->semaforoTMP);
			nuevo->pedido_extension=-1;
			//sacar la metadata
			metaTabla *metadata=leerMetadataTabla(nuevo->nombre);
			if(metadata==NULL){
				log_info(compaclog,"Error al abrir la metada, se terminara el hilo de %s.",nuevo->nombre);
				pthread_exit(NULL);
			}
			t_list *todosLosRegistros=list_create();
			contador=0;
			int cantMax=metadata->partitions;
			int binario=0;
			//leer Arch
			while(cantMax>contador){
				int extension;
				if(binario==0){
					//leer binarios
					extension=0;
				}else{
					//leerTempc
					extension=2;
				}
				escanearArchivo(nuevo->nombre,contador,extension,todosLosRegistros);
				if(contador+1==cantMax && binario==0){
					contador=-1;
					cantMax=cantTemp;
					binario++;
				}
				contador++;
			}
			log_info(compaclog,"Se terminaron de leer los tmps y bins de la tabla %s.",nuevo->nombre);
			contador=0;
			t_list *depurado=regDep(todosLosRegistros);
			//escribir
			//WAIT MUTEX DE BINARIOS DE LA TABLA
			sem_wait(&nuevo->semaforoBIN);
			nuevo->pedido_extension=0;
			while(contador<metadata->partitions){
				t_list *particion=filtrarPorParticion(depurado,contador,metadata->partitions);
				char *archP=nivelParticion(nuevo->nombre,contador,0);
				liberarParticion(archP);
				if(escribirParticion(archP,particion,1)==1){
					log_error(compaclog,"Error a la hora de escribir la compactacion de %s.",nuevo->nombre);
					free(archP);
					list_destroy(particion);
					borrarMetadataTabla(metadata);
					list_destroy(depurado);
					list_destroy_and_destroy_elements(todosLosRegistros,(void *)destroyRegistry);
					compactar(nuevo);
				}
				free(archP);
				list_destroy(particion);
				contador++;
			}
			log_info(compaclog,"se termino de escribir la compactacion de %s",nuevo->nombre);
			//SIGNAL MUTEX BINARIOS DE LA TABLA
			sem_post(&nuevo->semaforoBIN);
			//liberar TMPC
			contador=0;
			nuevo->pedido_extension=2;
			//WAIT MUTEX TMPC DE LA TABLA
			sem_wait(&nuevo->semaforoTMPC);
			while(cantTemp>contador){
				char *arch=nivelParticion(nuevo->nombre,contador,2);
				liberarParticion(arch);
				free(arch);
				contador++;
			}
			log_info(compaclog,"se eliminaron los archivos tmpC de la tabla %s",nuevo->nombre);
			//SIGNAL MUTEX TMPC DE LA TABLA
			sem_post(&nuevo->semaforoTMPC);
			list_destroy(depurado);
			borrarMetadataTabla(metadata);
			list_destroy_and_destroy_elements(todosLosRegistros,(void *)destroyRegistry);
			log_info(compaclog,"Fin de la compactacion de %s",nuevo->nombre);
			nuevo->pedido_extension=-1;
			sem_getvalue(&nuevo->borrarTabla,&seguir);
		}
	}
	pthread_exit(NULL);
}
void liberarTabDirectorio(Sdirectorio *nuevo){
	free(nuevo->nombre);
	liberarSemaforosTabla(nuevo);
	free(nuevo);
}
void cerrarTabDirectorio(Sdirectorio *nuevo){
	if(nuevo->pedido_extension<0){
		pthread_kill(nuevo->hilo,0);
	}else{
		pthread_join(nuevo->hilo,0);
	}
	free(nuevo->nombre);
	liberarSemaforosTabla(nuevo);
	free(nuevo);

}
void liberarDirectorioP(){
	list_destroy_and_destroy_elements(directorioP,(void *)cerrarTabDirectorio);
}

void semaforosTabla(Sdirectorio *nuevo){
	sem_init(&nuevo->semaforoBIN,0,1);
	sem_init(&nuevo->semaforoContarTMP,0,1);
	sem_init(&nuevo->semaforoTMP,0,1);
	sem_init(&nuevo->semaforoTMPC,0,1);
	sem_init(&nuevo->borrarTabla,0,1);
	sem_init(&nuevo->semaforoMeta,0,1);
	nuevo->pedido_extension=-1;
}
void liberarSemaforosTabla(Sdirectorio *nuevo){
	sem_destroy(&nuevo->semaforoBIN);
	sem_destroy(&nuevo->semaforoContarTMP);
	sem_destroy(&nuevo->semaforoTMP);
	sem_destroy(&nuevo->semaforoTMPC);
	sem_destroy(&nuevo->borrarTabla);
	sem_destroy(&nuevo->semaforoMeta);
}
Sdirectorio *obtenerUnaTabDirectorio(char *tabla){
	int contador=0;
	while(contador<list_size(directorioP)){
		Sdirectorio *es=list_get(directorioP,contador);
		if(string_equals_ignore_case(es->nombre,tabla)){
			return es;
		}
		contador++;
		es=NULL;
	}
	return NULL;
}
/*Sdirectorio *obtenerUnaTabDirectorio(char *tabla){
	bool tabla_directorio(Sdirectorio *es){
		return es->nombre == tabla;
	}
	return list_find(directorioP,(void *)tabla_directorio);
}*/
