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
		oldCrearMetaLFS(32,10,"Lissandra");
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

bool archivoYaAbierto(char *tabla,int nombreArch,char *extension){
	bool abierto(archAbierto *es){
			if(tabla==es->nombreTabla && extension==es->extension){
				return true;
			}
			return false;
		}
	return list_any_satisfy(tablaArchGlobal,(void *)abierto);
}
archAbierto *obtenerArch(char *tabla,int nombreArch,char *extension){
	bool abierto(archAbierto *es){
			if(tabla==es->nombreTabla && extension==es->extension){
				return true;
			}
			return false;
		}
	return list_find(tablaArchGlobal,(void *)abierto);
}
metaArch *abrirArchivo(char *tabla,int nombreArch,char *extension){
	metaArch *arch=NULL;

	if(archivoYaAbierto(tabla,nombreArch,extension)){
		archAbierto *obtenido=obtenerArch(tabla,nombreArch,extension);
		obtenido->contador+=1;
	}else{
		archAbierto *nuevo=malloc(sizeof(archAbierto));
		nuevo->contador=1;
		nuevo->extension=string_from_format("%s",extension);
		nuevo->nombreTabla=string_from_format("%s",tabla);
		list_add(tablaArchGlobal,nuevo);
	}
	char *path=nivelUnaTabla(tabla,0);
	char *path2=string_from_format("%s%s.%s",path,nombreArch,extension);
	free(path);
	//WAIT
	arch=leerMetaArch(path);
	free(path2);
	return arch;
}

void liberarArch(archAbierto *nuevo){
	free(nuevo->extension);
	free(nuevo->nombreTabla);
	free(nuevo);
}

int calcularIndexArch(char *tabla,int nombreArch,char *extension){
	int index=-1;
	bool abierto(archAbierto *es){
		index++;
			if(tabla==es->nombreTabla && extension==es->extension){
				return true;
			}
			return false;
		}
	list_iterate(tablaArchGlobal, (void*) abierto);
	return index;
}
void sacarArch(char *tabla,int nombreArch,char *extension){
	int index=calcularIndexArch(tabla,nombreArch,extension);
	archAbierto *victima=list_remove(tablaArchGlobal,index);
	liberarArch(victima);
}
void cerrarArchivo(char *tabla,int nombreArch,int extension, metaArch *arch){
	archAbierto *obtenido=obtenerArch(tabla,nombreArch,extension);
	obtenido->contador-=1;
	if(obtenido->contador==0){
		sacarArch(tabla,nombreArch,extension);
		//SIGNAL
	}
	borrarMetadataTabla(arch);
}
