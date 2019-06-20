/*
 * FileSystem.c
 *
 *  Created on: 9 jun. 2019
 *      Author: utnso
 */

#include "FileSystem.h"

void levantarDirectorio(){
	if(crearMontaje()==0){
		log_info(logger,"Se ha creado el montaje.");
	}

	if(crearNivelMetadata()==0){
		log_info(logger,"Se ha creado el nivel Metadata.");
	}
	if(crearNivelTablas()==0){
		log_info(logger,"Se ha creado el nivel Tablas.");
	}
	if(crearNivelBloques()==0){
		log_info(logger,"Se ha creado el nivel Bloques.");
	}
}

int crearMontaje(){
	char *montaje=obtenerMontaje();
	char **subCadena=string_split(montaje,"/");
	int i=2;
	char *path=malloc(strlen(raizDirectorio)+1);
	strcpy(path,raizDirectorio);
	while(subCadena[i]!=NULL){
		path=ponerBarra(path);
		path=realloc(path,strlen(path)+strlen(subCadena[i])+1);
		strcat(path,subCadena[i]);
		if(folderExist(path)==1){
			if(crearCarpeta(path)!=0){
				return 1;
			}
		}
		i++;
	}
	i=0;
	while(subCadena[i]!=NULL){
		free(subCadena[i]);
		i++;
	}
	free(montaje);
	free(subCadena);
	free(path);
	return 0;
}

int crearNivelMetadata(){
	char *path=nivelMetadata(0);
	if(folderExist(path)!=1){
		log_info(logger,"La carpeta Metadata ya existe.");
	}else{
		crearCarpeta(path);
	}
	free(path);
	path=nivelMetadata(1);
	if(archivoValido(path)==0){
		//crearMetaLFS();
		oldCrearMetaLFS(64,10,"Lissandra");
	}else{
		leerMetaLFS();
	}
	free(path);
	cargarBitmap();

	return 0;
}

int crearNivelTablas(){
	char *path=nivelTablas();
	if(folderExist(path)!=1){
		log_info(logger,"El directorio ya existe.");
	}else{
		if(crearCarpeta(path)==1){
			free(path);
			return 1;
		}
	}
	free(path);
	return 0;
}

int crearNivelBloques(){
	char *path=nivelBloques();
	if(folderExist(path)!=1){
		log_info(logger,"El directorio ya existe.");
	}else{
		if(crearCarpeta(path)==1){
			free(path);
			return 1;
		}
	}
	//crear todos los bloques .bin
	int i=0;
	while(i<metaLFS->cantBloques){
		char *pathBloque=rutaBloqueNro(i);
		FILE *bloque=fopen(pathBloque,"wb");
		if(bloque!=NULL){
			fclose(bloque);
		}
		i++;
		free(pathBloque);
	}
	free(path);
	return 0;
}
