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
	char *path=nivelUnaTabla(param_nameTable, 0);
	if(folderExist(path)==1){
		//En caso que no exista, informa el error y continúa su ejecución.
		log_error(logger,"No se puede hacer el insert porque no existe la tabla %s.", param_nameTable);
		perror("No se puede hacer el insert porque no existe la tabla %s.");
		free(path);
		return 1;
	}
	free(path);

	if(string_length(param_value)+1>configLissandra->tamValue){
		log_error(logger,"No se puede hacer el insert porque el value excede el tamanio permitido.");
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

char *lSelect(char *nameTable, u_int16_t key){
	char *path=nivelUnaTabla(nameTable, 0);
	char *valor=NULL;
	t_list *obtenidos=list_create();

	//Verificar que la tabla exista en el file system.
	if(folderExist(path)==1){
		log_error(logger,"No se puede hacer el insert porque no existe la tabla %s.", nameTable);
		free(path);
		list_destroy(obtenidos);
		return valor;
	}
	free(path);
	//Obtener la metadata asociada a dicha tabla.
	metaTabla *metadata= leerMetadataTabla(nameTable);

	//Calcular cual es la partición que contiene dicho KEY.
	int part=key % metadata->partitions;
	log_info(logger, "La key %i esta contenida en la particion %i.",key, part);

	//Escanear la partición objetivo (modo 0)
	path=nivelParticion(nameTable, part, 0);
	escanearArchivo(path,obtenidos);
	free(path);

	//Escanear todos los archivos temporales (modo 1)
	int cantDumps=contarArchivos(nameTable, 1); //PREGUNTAR DILEMA
	int i=0;
	while(i<cantDumps){
		path=nivelParticion(nameTable,cantDumps-1, 1);
		escanearArchivo(path, obtenidos);
		free(path);
		i++;
	}
	//Escanear los .tmpc si es necesario (modo 2)

	int cantTmpc=contarArchivos(nameTable, 2);
	i=0;
	while(i<cantTmpc){
		path=nivelParticion(nameTable,cantTmpc-1, 1);
		escanearArchivo(path, obtenidos);
		free(path);
		i++;
	}

	//Escanear la memtable
	t_list *aux;
	if(!list_is_empty(memtable)){
		Tabla *encontrada= find_tabla_by_name(nameTable);
		if(encontrada!=NULL){
			aux=filtrearPorKey(encontrada->registros,key);
			list_add_all(aux,obtenidos);
		}
	}
	//Comparar los timestamps
	if(!list_is_empty(aux)){
		if(existeKeyEnRegistros(aux,key)==1){
			t_list *filtrada;
			Registry *obtenido;
			filtrada=filtrearPorKey(aux,key);
			obtenido=regConMayorTime(filtrada);
			valor=malloc(strlen(obtenido->value)+1);
			strcpy(valor,obtenido->value);
			log_info(logger, valor);
		}else{
			log_info(logger,"No se ha encontrado el valor.");
		}
	}
	//FALTA LIBERAR EL METATABLA
	borrarMetadataTabla(metadata);
	//FALTA LIBERAR LA LISTA AUXILIAR
	list_destroy(aux);
	//FALTA LIBERAR LA LISTA DE OBTENIDOS Y TODOS LOS REGISTROS DE AHI DENTRO
	list_destroy_and_destroy_elements(obtenidos,(void *)destroyRegistry);
	return valor;
}

//*****************************************************************************************************
int create(char* nameTable, char* consistency , u_int16_t numPartition,long timeCompaction){
	char *nombre=string_duplicate(nameTable);
	string_to_upper(nombre);	//solo funciona si escribis en minuscula
	char *path=nivelUnaTabla(nombre, 0);
	//Verificar que la tabla no exista en el file system.
	//En caso que exista, se guardará el resultado en un archivo .log
	//y se retorna un error indicando dicho resultado.
	if(folderExist(path)==0){
		log_error(logger, "No se puede hacer el create porque ya existe la tabla %s.",nameTable);
		perror("La tabla ya existe.");
		free(path);
		return 1;
	}
	//Crear el directorio para dicha tabla.
	if(hayXBloquesLibres(numPartition)){
		if(crearCarpeta(path)==1){
			log_error(logger,"ERROR AL CREAR LA TABLA %s.",nombre);
			free(path);
			//liberar el semaforo de bloques ocupados
			return 1;
		}
		//Crear el archivo Metadata asociado al mismo.
		//Grabar en dicho archivo los parámetros pasados por el request.
		metaTabla *tabla=crearMetadataTabla(nombre,consistency,numPartition,timeCompaction);
		//Crear los archivos binarios asociados a cada partición de la tabla con sus bloques

		if(crearParticiones(tabla)==1){
			log_error(logger,"ERROR AL CREAR LAS PARTICIONES.");
			//liberar el semaforo de bloques ocupados
			return 1;
		}
		borrarMetadataTabla(tabla);
	}else{
		log_error(logger,"No hay %i bloques libres.\n",numPartition);
		//liberar el semaforo de bloques ocupados
		return 1;
	}
	list_add(directorio,nombre);
	free(path);
	return 0;
}

t_list *describe(char* nameTable,int variante){//PREGUNTAR, PORQUE 2 ATRIBUTOS, SI NAMETABLE ES NULL DEBERIA BASTAR
	t_list *tablas=list_create();
	if(variante==0){
		if(list_is_empty(directorio)){
			log_error(logger,"No hay ninguna tabla cargada en el sistema.");
		}else{
			//Recorrer el directorio de árboles de tablas
			//y descubrir cuales son las tablas que dispone el sistema.
			int cant=list_size(directorio);
			while(cant>0){
				char *tabla=list_get(directorio,cant-1);
				char *path=nivelUnaTabla(tabla,0);
				if(folderExist(path)==0){
					free(path);
					//Leer el archivo Metadata de dicha tabla.
					metaTabla *metadata= leerMetadataTabla(nameTable);
					list_add(tablas,metadata);
				}else{
					free(path);
				}
				cant--;
			}
		}
		//Retornar el contenido de dichos archivos Metadata.
		return tablas;
	}else{
		//Verificar que la tabla exista en el file system.
		char *path=nivelUnaTabla(nameTable,0);
		if(folderExist(path)==0){
			free(path);
			//Leer el archivo Metadata de dicha tabla.
			metaTabla *metadata= leerMetadataTabla(nameTable);
			list_add(tablas,metadata);
			//Retornar el contenido del archivo.
			return tablas;
		}else{
			log_error(logger, "No se puede hacer el describe porque no existe la tabla %s.", nameTable);
			free(path);
			return tablas;
		}
	}
}

void drop(char* nameTable){
	//Verificar que la tabla exista en el file system.

	char *path=nivelUnaTabla(nameTable,0);
	if(folderExist(path)==0){
		//eliminar archivos binarios, temporales, tempC y metadata
		//liberar bloques y aumentar el semaforo contador
		//sacar la tabla del directorio
		//Eliminar directorio
		borrarCarpeta(path);
	}else{
		log_error(logger, "No se puede hacer el drop porque no existe la tabla %s.", nameTable);
	}
	free(path);
}
