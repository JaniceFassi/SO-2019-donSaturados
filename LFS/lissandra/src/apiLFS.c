/*
 * api.c
 *
 *  Created on: 9 abr. 2019
 *      Author: utnso
 */

#include "apiLFS.h"

//API
int insert(char *param_nameTable, u_int16_t param_key, char *param_value, long param_timestamp){

	//Verificar que la tabla exista en el file system.
	//En caso que no exista, informa el error y continúa su ejecución.
	char *path=pathFinal(param_nameTable,1);
	if(folderExist(path)==1){
		log_info(logger,"No se puede hacer el insert porque no existe la tabla %s.", param_nameTable);
		free(path);
		return 1;
	}
	free(path);

	if(string_length(param_value)>atoi(config_get_string_value(config,"TAMVALUE"))){
		log_info(logger,"No se puede hacer el insert porque el value excede el tamanio permitido.");
		return 1;
	}

	/*Verificar si existe en memoria una lista de datos a dumpear.
	   De no existir, alocar dicha memoria.*/

	if(list_is_empty(memtable)){

		Tabla *nueva=crearTabla(param_nameTable,param_key,param_value,param_timestamp);
		list_add(memtable,nueva);
	}else{
		//Insertar en la memoria temporal del punto anterior una nueva entrada que contenga los datos enviados en la request.

		Tabla *encontrada= find_tabla_by_name(param_nameTable);

		if(encontrada==NULL){

			Tabla *nueva=crearTabla(param_nameTable,param_key,param_value,param_timestamp);
			list_add(memtable,nueva);
		}else{

			agregarRegistro(encontrada,param_key,param_value,param_timestamp);
		}
	}
		log_info(logger,"Se ha insertado el registro en la memtable.");
	return 0;
}

int archivoValido(char *path){				//Devuelve 0 esta vacio o si no existe, si no 1
	FILE *archB;
	archB= fopen(path, "r");
	if(archB!=NULL){
		fseek(archB, 0, SEEK_END);
		if (ftell(archB) == 0 ){
			fclose(archB);
			return 0;
		}
		else{
			fclose(archB);
			return 1;
		}
	}
	else {
		return 0;
	}
}

//**********************MODIFICADO
char *selectS(char* nameTable , u_int16_t key){
	char *path=pathFinal(nameTable,1);
	char *valor=NULL;
	Registry *obtenidoMem;
	Registry *obtenidoPart;
	Registry *obtenidoTemp;
	t_list *obtenidos=list_create();
	//printf("%s",obtenidoPart->value);
	//printf("%s",obtenidoTemp->value);

	//Verificar que la tabla exista en el file system.
	if(folderExist(path)==1){
		printf("No existe la tabla %s", nameTable);
		free(path);
		return valor;
	}
	free(path);

	//Obtener la metadata asociada a dicha tabla.
	metaTabla *metadata= leerArchMetadata(nameTable);

	//Calcular cual es la partición que contiene dicho KEY.
	int part=key % metadata->partitions;

	//Escanear la partición objetivo (modo 0), y todos los archivos temporales (modo 1)
	path=concatExtencion(nameTable,part,1);
	t_list *temp;
	if(archivoValido(path)==1){
		temp=leerTodoArchBinario(path);
		if(list_is_empty(temp)){
			obtenidoTemp=NULL;
			list_destroy(temp);
		}else{
			if(encontrarKeyDepu(temp,key)!=NULL){
				obtenidoTemp=encontrarKeyDepu(temp,key);
				list_add(obtenidos,obtenidoTemp);
			}else{
				obtenidoTemp=NULL;
			}
		}
	}

	free(path);
	path=concatExtencion(nameTable,part,0);
	t_list *bin;
	if(archivoValido(path)==1){
		bin=leerTodoArchBinario(path);

		if(list_is_empty(bin)){
			obtenidoPart=NULL;
			list_destroy(bin);
		}else{
			if(encontrarKeyDepu(bin,key)!=NULL){
				obtenidoPart=encontrarKeyDepu(bin,key);
				list_add(obtenidos,obtenidoPart);
			}else{
				obtenidoPart=NULL;
			}
		}
	}
	free(path);
	//y la memoria temporal de dicha tabla (si existe) buscando la key deseada.

	if(!list_is_empty(memtable)){
		Tabla *encontrada= find_tabla_by_name(nameTable);
		if(encontrada!=NULL){
			t_list *aux=filtrearPorKey(encontrada->registros,key);
			if(list_is_empty(aux)){
				list_destroy(aux);
				obtenidoMem=NULL;
			}else{
				obtenidoMem=keyConMayorTime(aux);
				list_add(obtenidos,obtenidoMem);
				list_destroy(aux);
			}
		}
	}else{
		obtenidoMem=NULL;
	}
	if(list_is_empty(obtenidos)){
		valor=NULL;
		return valor;
	}else{
		Registry *final=keyConMayorTime(obtenidos);
		valor=malloc(strlen(final->value)+1);
		strcpy(valor,final->value);
		destroyRegistry(final);
	}
		//Encontradas las entradas para dicha Key, se retorna el valor con el Timestamp más grande.
	//free(metadata->nombre);
	free(metadata->consistency);
	free(metadata);
	destroyRegistry(obtenidoMem);
	if(list_is_empty(bin)){
		//list_destroy(bin);
	}else{
		//list_destroy_and_destroy_elements(bin,(void *)destroyRegistry);
	}
	if(list_is_empty(temp)){
			list_destroy(temp);
		}else{
			//list_destroy_and_destroy_elements(temp,(void *)destroyRegistry);
		}
	log_info(logger,valor);
	return valor;
}
//*****************************************************************************************************
int create(char* nameTable, char* consistency , u_int16_t numPartition,long timeCompaction){
	//Verificar que la tabla no exista en el file system.
	//Por convención, una tabla existe si ya hay otra con el mismo nombre.
	//Para dichos nombres de las tablas siempre tomaremos sus valores en UPPERCASE (mayúsculas).
	//En caso que exista, se guardará el resultado en un archivo .log
	//y se retorna un error indicando dicho resultado.

	//string_to_upper(nameTable);									************NO FUNCA************
	char *path=pathFinal(nameTable,1);

		if(folderExist(path)==0){
			log_info(logger, "No se puede hacer el create porque ya existe la tabla %s.",nameTable);
			perror("La tabla ya existe.");
			free(path);
			return 1;
		}
	//Crear el directorio para dicha tabla.
	if(crearCarpeta(path)==1){
		log_info(logger,"ERROR AL CREAR LA TABLA %s.",nameTable);
		free(path);
		return 1;
	}

	//Crear el archivo Metadata asociado al mismo.
	//Grabar en dicho archivo los parámetros pasados por el request.
	crearArchMetadata(nameTable,consistency,numPartition,timeCompaction);
	//Crear los archivos binarios asociados a cada partición de la tabla y
	if(crearParticiones(nameTable,numPartition)==1){
		log_info(logger,"ERROR AL CREAR LAS PARTICIONES.");
		return 1;
	}
	//asignar a cada uno un bloque

	free(path);
	return 0;
}

t_list *describe(char* nameTable,int variante){//PREGUNTAR
	t_list *tablas=list_create();
	if(variante==0){
		//cuando no tiene el nombre de la tabla
		//Recorrer el directorio de árboles de tablas
		//y descubrir cuales son las tablas que dispone el sistema.

		//Leer los archivos Metadata de cada tabla.

		//Retornar el contenido de dichos archivos Metadata.
		return tablas;
	}else{
		//Verificar que la tabla exista en el file system.
		char *path=pathFinal(nameTable,1);
		if(folderExist(path)==0){
			free(path);
			path=pathFinal(nameTable,3);
			//Leer el archivo Metadata de dicha tabla.
			metaTabla *metadata= leerArchMetadata(path);
			//metadata->nombre=malloc(strlen(nameTable)+1);
			//strcpy(metadata->nombre,nameTable);
			list_add(tablas,metadata);
			//Retornar el contenido del archivo.
			free(path);
			return tablas;
		}else{
			log_info(logger, "No se puede hacer el describe porque no existe la tabla %s.", nameTable);
			free(path);
			return NULL;
		}

	}
}

void drop(char* nameTable){
	//Verificar que la tabla exista en el file system.

	char *path=pathFinal(nameTable,1);
	if(folderExist(path)==0){
		//Eliminar directorio y todos los archivos de dicha tabla.
		borrarCarpeta(path);

	}else{
		log_info(logger, "No se puede hacer el drop porque no existe la tabla %s.", nameTable);
	}
	free(path);
}
