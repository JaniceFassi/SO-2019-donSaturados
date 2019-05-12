/*
 * api.c
 *
 *  Created on: 9 abr. 2019
 *      Author: utnso
 */


#include "apiLFS.h"

//API
void insert(char *param_nameTable, u_int16_t param_key, char *param_value, long param_timestamp){

	//TODAVIA NO ESTA DECIDIDO
	//Verificar que la tabla exista en el file system.
	//En caso que no exista, informa el error y continúa su ejecución.
	if(folderExist(param_nameTable)==0){
		log_info(logger,"No existe esta tabla");
		return;
	}else{
	//	Obtener la metadata asociada a dicha tabla. PARA QUE?
		char *path=config_get_string_value(config, "PUNTO_MONTAJE");
		strcat(path,"/Tablas/");
		strcat(path,param_nameTable);
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

int selectS(char* nameTable , u_int16_t key, char *valor){
	//Verificar que la tabla exista en el file system.
	if(folderExist(nameTable)==0){
		log_info(logger,"No existe esa tabla");
		return 1;
	}else{
		//Obtener la metadata asociada a dicha tabla.
		char *path=config_get_string_value(config, "PUNTO_MONTAJE");
		strcat(path,"/Tablas/");
		strcat(path,nameTable);
		t_config* metadata=config_create(strcat(path,"/Metadata"));

		//Calcular cual es la partición que contiene dicho KEY.
		char* partition=config_get_string_value(metadata, "PARTITIONS");
		int part=key % atoi(partition);
		//Escanear la partición objetivo, todos los archivos temporales
		//y la memoria temporal de dicha tabla (si existe) buscando la key deseada.


		//Encontradas las entradas para dicha Key, se retorna el valor con el Timestamp más grande.
		config_destroy(metadata);

		}

	return 0;
}

void create(char* nameTable, char* consistency , u_int16_t numPartition,long timeCompaction){
	//Verificar que la tabla no exista en el file system.
	//Por convención, una tabla existe si ya hay otra con el mismo nombre.
	//Para dichos nombres de las tablas siempre tomaremos sus valores en UPPERCASE (mayúsculas).
	//En caso que exista, se guardará el resultado en un archivo .log
	//y se retorna un error indicando dicho resultado.
	if(folderExist(nameTable)!=0){
			log_info(logger, "Ya existe esa Tabla");
			perror("La tabla ya existe");
		}else{
	//Crear el directorio para dicha tabla.

	//Crear el archivo Metadata asociado al mismo.

	//Grabar en dicho archivo los parámetros pasados por el request.

	//Crear los archivos binarios asociados a cada partición de la tabla y
	//asignar a cada uno un bloque

		}

}

int describe(char* nameTable, t_list *tablas,int variante){//PREGUNTAR
	if(variante==0){
		//cuando no tiene el nombre de la tabla
		//Recorrer el directorio de árboles de tablas
		//y descubrir cuales son las tablas que dispone el sistema.

		//Leer los archivos Metadata de cada tabla.

		//Retornar el contenido de dichos archivos Metadata.

	}else{
		//Verificar que la tabla exista en el file system.
		if(folderExist(nameTable)!=0){
			//Eliminar directorio y todos los archivos de dicha tabla.
		}else{
			log_info(logger, "No existe esa Tabla");
		}
		//Leer el archivo Metadata de dicha tabla.

		//Retornar el contenido del archivo.

	}
	return 0;
}

void drop(char* nameTable){
	//Verificar que la tabla exista en el file system.

	if(folderExist(nameTable)!=0){
		//Eliminar directorio y todos los archivos de dicha tabla.
	}else{
		log_info(logger, "No existe esa Tabla");
	}
}
