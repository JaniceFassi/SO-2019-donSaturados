/*
 * FileSystem.c
 *
 *  Created on: 12 abr. 2019
 *      Author: utnso
 */

#include "FileSystem.h"

char *pathFinal(char *nombre, int principal,char *path){
	//0 ES LA CARP PRINCIPAL, 1 ES LA CARP DE LAS TABLAS, 2 PATH DE ARCH, 3 METADATA
	int base=string_length(path)+string_length(nombre)+2;
	if(principal==0){
		char *pathF=malloc(base);
		strcpy(pathF,path);
		strcat(pathF,nombre);
		return pathF;
	}
	char tab[]="Tablas/";
	base+=string_length(tab)+1;
	if(principal==1){
		char *pathF=malloc(base);
		strcpy(pathF,path);
		strcat(pathF,tab);
		strcat(pathF,nombre);
		return pathF;
	}
	char barra[]="/";
	base +=string_length(barra)+1;
	if(principal==2){
		char *pathF=malloc(base+string_length(tab)+2);
		strcpy(pathF,path);
		strcat(pathF,tab);
		strcat(pathF,nombre);
		strcat(pathF,barra);
		return pathF;
	}
	char metadata[]="METADATA";
	base += string_length(metadata)+1;
	if(principal==3){
		char *pathF=malloc(base+string_length(tab)+2);
		strcpy(pathF,path);
		strcat(pathF,tab);
		strcat(pathF,nombre);
		strcat(pathF,barra);
		strcat(pathF,metadata);
		return pathF;
	}
	return NULL;
}

int crearCarpeta(char* path){//CREA LA CARPETA
	if(mkdir(path,0777)<0){
		printf("No se pudo crear la Carpeta\n");
			return 1;
	}
	return 0;
}
int folderExist(char* path){ //Verifica si existe la carpeta, si no existe devuelve 1

	struct stat st = {0};
	if (stat(path, &st) == -1){
		printf("No existe la Carpeta\n");
		return 1;
	}
	return 0;
}
int borrarCarpeta(char *path){//BORRA LA CARPETA

	if(rmdir(path)<0){
		printf("No se pudo borrar la Carpeta\n");
		return 1;
	}
	return 0;
}

int crearParticiones(char *path, int cantidad){
	FILE* arch; //BINARIO

	while(cantidad>0){
		char *extencion=".bin";
		char *aux=string_duplicate(path);
		strcat(aux,string_itoa(cantidad));
		strcat(aux,extencion);
		if((arch= fopen(path,"wb"))<0){
			printf("Error al crear la particion numero %i de la tabla\n",cantidad);
			return 1;
		}
		fclose(arch);
		cantidad --;
	}
	return 0;
}

void crearArchMetadata(char* path, char* consistency , u_int16_t numPartition,long timeCompaction){
	metaTabla *nuevo=malloc(sizeof(metaTabla));
	nuevo->compaction_time=timeCompaction;
	nuevo->consistency= consistency;
	printf("%s",nuevo->consistency);
	nuevo->partitions= numPartition;
	printf("%i",nuevo->partitions);
	FILE* metadata= fopen(path,"wb"); //BINARIO
	//FILE* metadata= fopen(path,"w");    //TXT
	fclose(metadata);
	t_config *metaTab=config_create(path);
	config_set_value(metaTab,"CONSISTENCY",nuevo->consistency);
	char* cantParticiones = string_itoa(nuevo->partitions);
	config_set_value(metaTab,"PARTITIONS",cantParticiones);
	char* tiempoCompact = string_from_format("%ld",nuevo->compaction_time);
	config_set_value(metaTab,"COMPACTION_TIME",tiempoCompact);
	config_save(metaTab);
	config_destroy(metaTab);
	free(cantParticiones);
	free(tiempoCompact);
	free(nuevo);
}

metaTabla *leerArchMetadata(char *path){
	t_config *metaTab=config_create(path);
	metaTabla *nuevo=malloc(sizeof(metaTabla));
	nuevo->compaction_time=config_get_long_value(metaTab, "COMPACTION_TIME");
	char *aux=config_get_string_value(metaTab, "CONSISTENCY");
	nuevo->consistency=malloc(string_length(aux));
	strcpy(nuevo->consistency,aux);
	nuevo->partitions= config_get_int_value(metaTab, "PARTITIONS");
	config_destroy(metaTab);
	return nuevo;
}

//////////////////////
//FUNCIONES ASOCIADAS AL TAD REGISTRO
Registry *createRegistry(char *table, u_int16_t key, char *val, long time){
	Registry *data = malloc(sizeof(Registry));
	data->timestamp = time;
	data->key = key;
	data ->value=strdup(val);
	data ->name=strdup(table);
	return data;
}
void destroyRegistry(Registry *self) {
    free(self->name);
    free(self->value);
    free(self);
}
Registry *getList(){
	Registry *data = malloc(sizeof(Registry));
	data= list_get(memtable,0);
	return data;
}
