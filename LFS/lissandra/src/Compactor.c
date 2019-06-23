/*
 * Compactor.c
 *
 *  Created on: 12 abr. 2019
 *      Author: utnso
 */

#include "Compactor.h"
int pedirBloques(int cantidad, char **array){
	if(!hayXBloquesLibres(cantidad)){
		//COMPLETAR
		log_info(logger,"No hay suficientes bloques como para realizar el dump, se perderán los datos de la memtable.");
		return 1;
	}
	int i=0;
	while(i<cantidad){
		int bloqueVacio=obtenerBloqueVacio();
		array[i]=string_itoa(bloqueVacio);
		i++;
	}
	return 0;
}
void liberarArraydeBloques(char **array){
	int i=0;
	while(array[i]!=NULL){
		desocuparBloque(atoi(array[i]));
		free(array[i]);
		i++;
	}
}
int escribirParticion(char *path,t_list *lista){
	char *buffer=cadenaDeRegistros(lista);
	int largo=strlen(buffer)+1;
	//calcular cant bloques
	int bloquesNecesarios=largo/metaLFS->tamBloques;
	if(largo%metaLFS->tamBloques!=0){
		bloquesNecesarios++;
	}
	char **arrayBlock=malloc(sizeof(int)*bloquesNecesarios);
	//pedir x cant bloques y guardarlas en un char
	if(pedirBloques(bloquesNecesarios,arrayBlock)==1){
		log_error(logger,"error al pedir los bloques necesarios");
		return 1;
	}

	if(crearMetaArchivo(path, largo, arrayBlock,bloquesNecesarios)==1){
		log_info(logger,"ERROR AL CREAR LA METADATA DEL ARCHIVO.\n");
		liberarArraydeBloques(arrayBlock);
		free(arrayBlock);
		return 1;
	}

	escribirBloque(buffer,arrayBlock);//aca se libera el buffer

	for(int f=0;f<bloquesNecesarios;f++){
		free(arrayBlock[f]);
	}
	free(arrayBlock);

	return 0;
}
int dump(){
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
			if(escribirParticion(ruta,dumpTabla->registros)==1){
				log_error(logger,"error al escribir el dump");
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
void limpiarArchivo(char* pathArchivo){
	   FILE* fd;
	   fd = fopen((char*)pathArchivo,"wb");
	   fclose(fd);
}
void limpiarBloque(char* nroBloque){
	char* pathBloque=rutaBloqueNro(atoi(nroBloque));
	limpiarArchivo(pathBloque);
	desocuparBloque(atoi(nroBloque));
	free(pathBloque);
}

void liberarParticion(char *path){
	if(archivoValido(path)==1){
		metaArch *archivoAbierto=leerMetaArch(path);
		int cantBloques=0;
		while(archivoAbierto->bloques[cantBloques]!=NULL){
			limpiarBloque(archivoAbierto->bloques[cantBloques]);
		}
		borrarMetaArch(archivoAbierto);
		eliminarArchivo(path);
	}else{
		eliminarArchivo(path);
	}
}
int renombrarTemp_TempC(char *path){
	char *pathC=string_duplicate(path);
	pathC=realloc(pathC,strlen(pathC)+2);
	strcat(pathC,"c");
	if(rename(path,pathC)!=0){
		log_error(logger,"No se pudo renombrar el archivo");
		free(pathC);
		return 1;
	}
	free(pathC);
	return 0;
}

t_list *filtrarPorParticion(t_list *lista,int particion,int cantPart){
	t_list *nueva=list_create();
		void numParticion(Registry *compara){
			int resto= compara->key % cantPart;
			if(resto==particion){
				list_add(nueva,compara);
			}
		}
	list_iterate(lista,(void *)numParticion);
	return nueva;
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
	while(cantMax>contador){
		char *arch;
		if(binario==0){
			//leer binarios
			arch=nivelParticion(nombreTabla,contador,0);
			if(contador+1==cantMax){
				contador=0;
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
		if(escribirParticion(archP,particion)==1){
			log_error(logger,"Error a la hora de escribir la compactacion");
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
