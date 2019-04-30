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

	//TODAVIA NO ESTA DECIDIDO
	//Verificar que la tabla exista en el file system.
	//En caso que no exista, informa el error y continúa su ejecución.
	char *path=config_get_string_value(config, "PUNTO_MONTAJE");
	strcat(path,"Tables/");
	strcat(path,param_nameTable);
	if(folderExist(path)==0){
		log_info(logger,"No existe esta tabla");
		return;
	}else{
	//	Obtener la metadata asociada a dicha tabla. PARA QUE?

		t_config* metadata=config_create(strcat(path,"/Metadata"));

	/*Verificar si existe en memoria una lista de datos a dumpear.
	   De no existir, alocar dicha memoria.*/


	//El Timestamp es opcional. En caso que no este, se usará el valor actual del Epoch UNIX.
	if(param_timestamp==NULL){//No se muy bien como hacer lo del timestamp
		param_timestamp=time(NULL);
	}

	//Insertar en la memoria temporal del punto anterior una nueva entrada que contenga los datos enviados en la request.

		int ant=list_size(memtable);
	Registry *data = createRegistry(param_nameTable, param_key, param_value, param_timestamp);

	list_add(memtable, data);
	int tam= list_size(memtable);

	if(tam>ant){
		char log[]="se agrego el registro a la lista";
		log_info(logger,log);
	}
	}

}

char* selectS(char* nameTable , u_int16_t key){
	char** valor;
	//Verificar que la tabla exista en el file system.
	char *path=config_get_string_value(config, "PUNTO_MONTAJE");
	strcat(path,"Tables/");
	strcat(path,nameTable);
	if(folderExist(path)==0){
		log_info(logger,"No existe esa tabla");
		return *valor;
	}else{
		//Obtener la metadata asociada a dicha tabla.
		t_config* metadata=config_create(strcat(path,"/Metadata"));

		//Calcular cual es la partición que contiene dicho KEY.
		char* partition=config_get_string_value(metadata, "PARTITIONS");
		int part=key % atoi(partition);
		//Escanear la partición objetivo, todos los archivos temporales
		//y la memoria temporal de dicha tabla (si existe) buscando la key deseada.


		//Encontradas las entradas para dicha Key, se retorna el valor con el Timestamp más grande.
		config_destroy(metadata);

		}

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
