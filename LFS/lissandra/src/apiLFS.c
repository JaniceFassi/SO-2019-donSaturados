/*
 * api.c
 *
 *  Created on: 9 abr. 2019
 *      Author: utnso
 */


#include "apiLFS.h"
#include "Lissandra.h"

void api(op_code option){

	}

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

//API
void insert(char *param_nameTable, u_int16_t param_key, char *param_value, long param_timestamp){
	int ant=list_size(memtable);
	Registry *data = createRegistry(param_nameTable, param_key, param_value, param_timestamp);
	//char *row;
	//FILE *f=fopen("home/utnso/PruebaLissandra/Tabla.txt","w");

	list_add(memtable, data);
	int tam= list_size(memtable);

	if(tam>ant){
		char log[]="se agrego el registro a la lista";
		log_info(logger,log);
	}

	//node=list_get(memtable,0);

	//strcpy(row,node->value);

	//printf("%s",row);

	//fprintf();

	//fclose(f);
}

char* selectS(char* nameTable , u_int16_t key){
	char** valor;
	return *valor;
}

void create(char* nameTable, char* consistency , u_int16_t numPartition,long timeCompaction){

}

t_list* describe(char* nameTable){//PREGUNTAR
	t_list* arch;
	return arch;

}

void drop(char* nameTable){

}
