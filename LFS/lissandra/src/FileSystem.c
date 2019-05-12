/*
 * FileSystem.c
 *
 *  Created on: 12 abr. 2019
 *      Author: utnso
 */

#include "FileSystem.h"
int carpTabla(char* puntoMontaje){
	strcat(puntoMontaje,"/Tablas");
	if(mkdir(puntoMontaje,0777)<0){
		log_info(logger,"Error al crear la Tabla principal");
		return 1;
	}
	return 0;
}
int folderExist(char* name){ //Verifica si existe la carpeta, si no existe devuelve 0
	struct stat st = {0};
	char *path=config_get_string_value(config, "PUNTO_MONTAJE");
	strcat(path,"/Tablas/");
	strcat(path,name);
	if (stat(path, &st) == -1){
		return 0;
	}else{
		return 1;
	}
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
