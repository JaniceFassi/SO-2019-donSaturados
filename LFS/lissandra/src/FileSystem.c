/*
 * FileSystem.c
 *
 *  Created on: 12 abr. 2019
 *      Author: utnso
 */

#include "FileSystem.h"

char *pathFinal(char *nombre, int principal){//0 ES LA CARP PRINCIPAL, 1 ES LA CARP DE LAS TABLAS, 2 PATH DE ARCH

	char *path=config_get_string_value(config, "PUNTO_MONTAJE");

	if(principal==0){
		strcat(path,nombre);
		return path;
	}else{
		char *tab="Tablas/";
		strcat(path,tab);
		strcat(path,nombre);

		if(principal==1){
			return path;
		}else{
			char *barra="/";
			strcat(path,barra);
			return path;
		}
	}
}
int crearCarpeta(char* nombre, int principal){//CREA LA CARPETA

	char *path=pathFinal(nombre,principal);

	if(mkdir(path,0777)<0){
		log_info(logger,"Error al crear la Carpeta %s",nombre);
		return 1;
	}else{
		return 0;
	}
}
int folderExist(char* name, int principal){ //Verifica si existe la carpeta, si no existe devuelve 1

	struct stat st = {0};
	char *path=pathFinal(name,principal);

	if (stat(path, &st) == -1){
		log_info(logger,"No existe la Carpeta %s",name);
		return 1;
	}else{
		return 0;
	}
}
int borrarCarpeta(char *nombre, int principal){//BORRA LA CARPETA

	char *path=pathFinal(nombre,principal);

	if(rmdir(path)<0){
		log_info(logger,"Error al borrar la Carpeta %s",nombre);
		return 1;
	}else{
		return 0;
	}
}

int crearParticiones(char *nombre, int cantidad){
	char *path=pathFinal(nombre,2);
	while(cantidad>0){
		int arch;
		char *extencion=".bin";
		char *aux=string_duplicate(path);
		strcat(aux,string_itoa(cantidad));
		strcat(aux,extencion);
		if((arch=creat(aux,S_IRWXU))<0){
			log_info(logger,"Error al crear la particion numero %i de la tabla %s\n",cantidad,nombre);
			return 1;
		}
		close(arch);//NO SE SI ES NECESARIO
		cantidad --;
	}
	return 0;
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
