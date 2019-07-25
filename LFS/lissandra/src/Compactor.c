
#include "Compactor.h"

void *dump(){
	while(1){
		usleep(configLissandra->tiempoDump*1000);
		sem_wait(sem_dump);
		if(list_is_empty(memtable)){
			//log_info(dumplog,"la memtable esta vacia por lo cual no se hace el dump");
			sem_post(sem_dump);
		}else{
			sem_wait(criticaMemtable);
			t_list *dump=list_duplicate(memtable);
			sem_post(criticaMemtable);
			int cant=list_size(dump);
			log_info(logger,"se empezo a hacer el dump");
			while(cant>0){
				Tabla *dumpTabla=list_get(dump,cant-1);
				char *path=nivelUnaTabla(dumpTabla->nombre,0);
				if(folderExist(path)==0){
					//log_info(dumplog,"se empezo a hacer el dump de %s",dumpTabla->nombre);
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
				free(path);
				cant--;
			}
			log_info(logger,"se termino de hacer el dump");
			sem_wait(criticaMemtable);
			list_clean(memtable);
			sem_post(criticaMemtable);
			list_destroy_and_destroy_elements(dump,(void *)liberarTabla);
		}
		sem_post(sem_dump);
	}
	pthread_exit(NULL);
}
/*
void *dump(){
	//ACA IRIA EL WAIT MUTEX DE LA MEMTABLE
	while(1){
		usleep(configLissandra->tiempoDump*1000);
		sem_wait(sem_dump);
		sem_wait(criticaMemtable);
		if(list_is_empty(memtable)){
			log_info(dumplog,"la memtable esta vacia por lo cual no se hace el dump");
			sem_post(criticaMemtable);
			sem_post(sem_dump);
		}else{
			t_list *dump=list_duplicate(memtable);
			list_clean(memtable);
			int cant=list_size(dump);
			while(cant>0){
				log_info(logger,"se empezo a hacer el dump");
				Tabla *dumpTabla=list_get(dump,cant-1);
				char *path=nivelUnaTabla(dumpTabla->nombre,0);
				if(folderExist(path)==0){
					//log_info(dumplog,"se empezo a hacer el dump de %s",dumpTabla->nombre);
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
				log_info(logger,"se termino de hacer el dump");
			}
			list_destroy(dump);
		}
		//ACA IRIA EL SIGNAL MUTEX DE LA MEMTABLE
		sem_post(criticaMemtable);
		sem_post(sem_dump);
	}
	//return 0;
	pthread_exit(NULL);
}
*/
void *compactar(Sdirectorio* nuevo){
	while(nuevo->terminar){
		usleep(nuevo->time_compact*1000);
		sem_wait(&nuevo->semaforoCompactor);
		nuevo->pedido_extension=3;
		log_info(compaclog,"Se empezo a hacer la compactacion de la tabla %s",nuevo->nombre);
		char *path=nivelUnaTabla(nuevo->nombre,0);
		if(folderExist(path)==1){
			log_error(compaclog,"No se puede hacer la compactacion por que la tabla %s ya no existe",nuevo->nombre);
			free(path);
			return NULL;
		}
		free(path);
		sem_wait(&nuevo->semaforoContarTMP);
		int cantTemp=contarArchivos(nuevo->nombre,1);
		//Cambiar nombre de tmp a tmpc
		if(cantTemp==0){
			log_info(compaclog,"No hay datos para la compactacion de %s.",nuevo->nombre);
			sem_post(&nuevo->semaforoContarTMP);
		}else{
			int contador=0;
			// SEMAFORO WAIT PEDIDO RENOMBRAR
			nuevo->pedido_extension=1;
			sem_wait(&nuevo->semaforoTMP);
			while(cantTemp>contador){
				char *arch=nivelParticion(nuevo->nombre,contador,1);
				if(renombrarTemp_TempC(arch)==1){
					log_error(compaclog,"No se puedo renombrar el tmp de la tabla %s.",nuevo->nombre);
				}
				free(arch);
				contador++;
			}
			//log_info(compaclog,"Se pudieron renombrar los tmps de la tabla %s.",nuevo->nombre);
			sem_post(&nuevo->semaforoTMP);
			int valor;
			sem_getvalue(&nuevo->archivoBloqueado,&valor);
			if(valor==0){
				sem_post(&nuevo->archivoBloqueado);
			}
			sem_post(&nuevo->semaforoContarTMP);
			//SIGNAL WAIT PEDIDO RENOMBRAR
			nuevo->pedido_extension=-1;
			//sacar la metadata
			metaTabla *metadata=leerMetadataTabla(nuevo->nombre);
			//log_info(compaclog,"Se abrio la metadata %s.",nuevo->nombre);
			if(metadata==NULL){
				log_info(compaclog,"Error al abrir la metada, se terminara el hilo de %s.",nuevo->nombre);
				pthread_exit(NULL);
				return NULL;
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
					//log_info(compaclog,"Se terminaron de leer los bins de la tabla %s.",nuevo->nombre);
					contador=-1;
					cantMax=cantTemp;
					binario++;
				}
				contador++;
			}
			//log_info(compaclog,"Se terminaron de leer los tmps y bins de la tabla %s.",nuevo->nombre);
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
					pthread_exit(NULL);
					//compactar(nuevo);
				}
				free(archP);
				list_destroy(particion);
				contador++;
			}
			//log_info(compaclog,"se termino de escribir la compactacion de %s",nuevo->nombre);
			//SIGNAL MUTEX BINARIOS DE LA TABLA
			sem_post(&nuevo->semaforoBIN);
			sem_getvalue(&nuevo->archivoBloqueado,&valor);
			if(valor==0){
				sem_post(&nuevo->archivoBloqueado);
			}
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
			//log_info(compaclog,"se eliminaron los archivos tmpC de la tabla %s",nuevo->nombre);
			//SIGNAL MUTEX TMPC DE LA TABLA
			sem_post(&nuevo->semaforoTMPC);
			sem_getvalue(&nuevo->archivoBloqueado,&valor);
			if(valor==0){
				sem_post(&nuevo->archivoBloqueado);
			}
			nuevo->pedido_extension=-1;
			list_destroy(depurado);
			borrarMetadataTabla(metadata);
			list_destroy_and_destroy_elements(todosLosRegistros,(void *)destroyRegistry);
			log_info(compaclog,"Fin de la compactacion de %s",nuevo->nombre);
		}
		sem_post(&nuevo->semaforoCompactor);
	}
	return NULL;
}

void liberarTabDirectorio(Sdirectorio *nuevo){
	free(nuevo->nombre);
	liberarSemaforosTabla(nuevo);
	free(nuevo);
}

void cerrarHiloCompactor(Sdirectorio *nuevo){
	nuevo->terminar=0;
	sem_wait(&nuevo->semaforoCompactor);
	pthread_cancel(nuevo->hilo);
	sem_post(&nuevo->semaforoCompactor);
	//log_info(logger,"se termino el hilo de %s",nuevo->nombre);
}

void liberarDirectorioP(){
	int cant=list_size(directorioP);
	for(int i=0;i<cant;i++){
		Sdirectorio *tabla=list_get(directorioP,i);
		if(tabla!=NULL){
			cerrarHiloCompactor(tabla);
		}
	}
	list_destroy_and_destroy_elements(directorioP,(void*)liberarTabDirectorio);
}

void semaforosTabla(Sdirectorio *nuevo){
	sem_init(&nuevo->semaforoBIN,0,1);
	sem_init(&nuevo->semaforoContarTMP,0,1);
	sem_init(&nuevo->semaforoTMP,0,1);
	sem_init(&nuevo->semaforoTMPC,0,1);
	sem_init(&nuevo->semaforoMeta,0,1);
	sem_init(&nuevo->semaforoCompactor,0,1);
	sem_init(&nuevo->archivoBloqueado,0,1);
	nuevo->pedido_extension=-1;
}

void liberarSemaforosTabla(Sdirectorio *nuevo){
	sem_destroy(&nuevo->semaforoBIN);
	sem_destroy(&nuevo->semaforoContarTMP);
	sem_destroy(&nuevo->semaforoTMP);
	sem_destroy(&nuevo->semaforoTMPC);
	sem_destroy(&nuevo->semaforoMeta);
	sem_destroy(&nuevo->semaforoCompactor);
	sem_destroy(&nuevo->archivoBloqueado);
}

Sdirectorio *obtenerUnaTabDirectorio(char *tabla){
	int contador=0;
	while(contador<list_size(directorioP)){
		Sdirectorio *es=list_get(directorioP,contador);
		if(es!=NULL){
			if(string_equals_ignore_case(es->nombre,tabla)){
				return es;
			}
		}
		contador++;
	}
	return NULL;
}
