/*
 * FileSystem.c
 *
 *  Created on: 9 jun. 2019
 *      Author: utnso
 */

#include "FileSystem.h"

void levantarDirectorio(){
	char *montaje=obtenerMontaje();
	if(crearMontaje(montaje)==0){
		log_info(logger,"Se ha creado el directorio.");
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

int crearMontaje(char *montaje){
	char **subCadena=string_split(montaje,"/");
	int i=2;
	char *path=malloc(strlen(raizDirectorio)+1);
	strcpy(path,raizDirectorio);
	while(subCadena[i]!=NULL){
		path=realloc(path,strlen(path)+strlen(subCadena[i])+1);
		strcpy(path,ponerBarra(path));
		strcat(path,subCadena[i]);
		strcpy(path,ponerBarra(path));
		if(crearCarpeta(path)!=0){
			return 1;
		}
		i++;
	}
	i=0;
	while(subCadena[i]!=NULL){
		free(subCadena[i]);
		i++;
	}
	//free(path);
	return 0;
}

int crearNivelMetadata(char *path, int modo){
	strcpy(path,nivelMetadata(0));
	if(folderExist(path)!=1){
		log_info(logger,"El directorio ya existe.");
		return 1;
	}else{
		crearCarpeta(path);
	}
	path=nivelMetadata(1);
	crearMetaLFS(path,64,5192,"Lissandra");

	path=nivelMetadata(2);
	//crear el archivo bitmap

	return 0;

}

int crearNivelTablas(char *path){
	strcpy(path,nivelTablas());
	if(folderExist(path)!=1){
		log_info(logger,"El directorio ya existe.");
		return 1;
	}else{
		crearCarpeta(path);
	}
	return 0;
}

int crearNivelBloques(char *path){
	strcpy(path,nivelBloques());
	if(folderExist(path)!=1){
		log_info(logger,"El directorio ya existe.");
		return 1;
	}else{
		crearCarpeta(path);
	}
	char *pathMetadata=nivelMetadata(1);
	leerMetaLFS(pathMetadata);
	//crear todos los bloques .bin
	int i=0;
	while(i<=metaLFS->cantBloques){
		char *pathBloque=rutaBloqueNro(i);
		FILE *bloque=fopen(pathBloque,"rb");
		if(bloque!=NULL){
			fclose(bloque);
		}
		i++;
		free(pathBloque);
	}
	free(pathMetadata);
	return 0;
}
