/*
 * FileSystem.c
 *
 *  Created on: 9 jun. 2019
 *      Author: utnso
 */

#include "FileSystem.h"

void levantarDirectorio(){
	if(crearMontaje()==0){									//CREA LAS CARPETAS DEL MONTAJE SI NO EXISTEN
		log_info(logger,"Se levanto el montaje.");
	}

	if(crearNivelMetadata()==0){							//CREA LA CARPETA METADATA CON EL METADATA.BIN y el BITMAP.BIN SI NO EXISTE
		log_info(logger,"Se levanto el nivel Metadata.");
	}
	if(crearNivelTablas()==0){								//CREA LA CARPETA TABLAS SI NO EXISTE
		log_info(logger,"Se levanto el nivel Tablas.");
	}
	if(crearNivelBloques()==0){								//CREA LA CARPETA BLOQUES CON LA CANTIDAD DE BLOQUES NECESARIOS SI NO EXISTE
		log_info(logger,"Se levanto el nivel Bloques.");
	}
}

int crearMontaje(){
	char *montaje=obtenerMontaje();					//Si la opcion es 0 se devuelve el montaje, si no se concatena a /home/utnso
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
	//free(montaje);
	free(subCadena);
	free(path);
	return 0;
}

int crearNivelMetadata(){
	char *path=nivelMetadata(0);
	int crear=0;
	if(folderExist(path)==1){
		crearCarpeta(path);
		crear=1;
	}
	free(path);
	path=nivelMetadata(1);
	if(archivoValido(path)==0){						//SI NO EXISTE EL METADATA.BIN CREA TODO
		//crearMetaLFS();							//PIDE LOS VALORES POR CONSOLA. SE PUEDE SACAR?
		oldCrearMetaLFS(64,4096,"Lissandra");
	}else{
		leerMetaLFS();
	}
	free(path);
	if(crear){
		cargarBitmap();
	}
	return 0;
}

int crearNivelTablas(){
	struct dirent *file;
	struct stat myStat;
	char *path=nivelTablas();
	if(folderExist(path)==1){
		if(crearCarpeta(path)==1){
			free(path);
			return 1;
		}
	}else
	{
		char *dirTablas=nivelTablas();
		DIR *dir=opendir(dirTablas);							//Examina si ya hay tablas en el fs, las agrega al directorio, comienza la compactacion de c/u
		if(dir!=NULL){
			while((file=readdir(dir))!=NULL){
				stat(file->d_name, &myStat);
				if ( (strcmp(file->d_name, ".")!=0) && (strcmp(file->d_name, "..")!=0) ){
					log_info(logger,"%s es una tabla.",file->d_name);
					metaTabla *metadata=levantarMetadataTabla(file->d_name);
					Sdirectorio *uno=malloc(sizeof(Sdirectorio));
					uno->nombre=malloc(strlen(file->d_name)+1);
					uno->time_compact=metadata->compaction_time;
					strcpy(uno->nombre,file->d_name);
					semaforosTabla(uno);
					//sem_wait(criticaDirectorio);
					list_add(directorioP,uno);
					pthread_create(&uno->hilo, NULL, &compactar,uno);
					//sem_post(criticaDirectorio);
					borrarMetadataTabla(metadata);
				}
			}
			closedir(dir);
		}
		free(dirTablas);
	}
	free(path);
	return 0;
}

int crearNivelBloques(){
	char *path=nivelBloques();
	if(folderExist(path)==1){
		if(crearCarpeta(path)==1){
			free(path);
			return 1;
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
	}
	free(path);
	return 0;
}

/*
bool archivoYaAbierto(char *tabla,int extension){
	int contador=0;
	while(contador<list_size(tablaArchGlobal)){
		archAbierto *es=list_get(tablaArchGlobal,contador);
		if(string_equals_ignore_case(es->nombreTabla,tabla)){
			if(extension==es->extension){
				return true;
			}
		}
		contador++;
	}
	return false;
}

*/
bool archivoYaAbierto(char *tabla,int extension){
	bool abierto(archAbierto *es){
			if(string_equals_ignore_case(es->nombreTabla,tabla)){
				if(extension==es->extension){
					return true;
				}
			}
			return false;
		}
	return list_any_satisfy(tablaArchGlobal,(void *)abierto);
}
/*
archAbierto *obtenerArch(char *tabla, int extension){
	bool abierto(archAbierto *es){
			if(tabla==es->nombreTabla && extension==es->extension){
				return true;
			}
			return false;
		}
	return list_find(tablaArchGlobal,(void *)abierto);
}*/
archAbierto *obtenerArch(char *tabla, int extension){
	int contador=0;
	while(contador<list_size(tablaArchGlobal)){
		archAbierto *es=list_get(tablaArchGlobal,contador);
		if(string_equals_ignore_case(es->nombreTabla,tabla)){
			if(extension==es->extension){
				return es;
			}
		}
		contador++;
	}
	return NULL;
}
void nuevoArch(char *tabla, int extension){
	if(archivoYaAbierto(tabla,extension)){
		archAbierto *obtenido=obtenerArch(tabla,extension);
		obtenido->contador+=1;
	}else{
		archAbierto *nuevo=malloc(sizeof(archAbierto));
		nuevo->contador=1;
		nuevo->extension=extension;
		nuevo->nombreTabla=string_from_format("%s",tabla);
		list_add(tablaArchGlobal,nuevo);
	}
}

void liberarArch(archAbierto *nuevo){
	free(nuevo->nombreTabla);
	free(nuevo);
}

int calcularIndexArch(char *tabla,int extension){
	int index=0;
	bool abierto(archAbierto *es){
			if(tabla==es->nombreTabla && extension==es->extension){
				return true;
			}
			index++;
			return false;
		}
	list_iterate(tablaArchGlobal, (void*) abierto);
	return index;
}

void sacarArch(char *tabla,int extension){
	int index=calcularIndexArch(tabla,extension);
	archAbierto *victima=list_remove(tablaArchGlobal,index-1);
	liberarArch(victima);
}
