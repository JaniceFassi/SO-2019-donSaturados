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
		Tabla *encontrada= find_tabla_by_name_in(param_nameTable, memtable);

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
		log_error(logger,"No se puede hacer el select porque no existe la tabla %s.", nameTable);
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
		path=nivelParticion(nameTable,i, 1);
		escanearArchivo(path, obtenidos);
		free(path);
		i++;
	}
	//Escanear los .tmpc si es necesario (modo 2)

	int cantTmpc=contarArchivos(nameTable ,2);
	i=0;
	while(i<cantTmpc){
		path=nivelParticion(nameTable,i, 1);
		escanearArchivo(path, obtenidos);
		free(path);
		i++;
	}

	//Escanear la memtable
	t_list *aux;
	if(!list_is_empty(memtable)){
		Tabla *encontrada= find_tabla_by_name_in(nameTable, memtable);
		if(encontrada!=NULL){
			aux=filtrearPorKey(encontrada->registros,key);
			list_add_all(aux,obtenidos);
		}
	}else
	{
		aux=list_create();
		list_add_all(aux,obtenidos);
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
			//FALTA LIBERAR LA FILTRADA
			list_destroy(filtrada);
		}else{
			log_info(logger,"No se ha encontrado el valor.");
		}
	}else
	{
		log_info(logger,"No se ha encontrado el valor.");
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
		free(path);
		free(nombre);
		return 1;
	}
	//Crear el directorio para dicha tabla.
	if(hayXBloquesLibres(numPartition)){
		if(crearCarpeta(path)==1){
			log_error(logger,"ERROR AL CREAR LA CARPETA %s.",nombre);
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
		free(path);
		free(nombre);
		//liberar el semaforo de bloques ocupados
		return 1;
	}
	list_add(directorio,nombre);
	log_info(logger,"Se ha creado la tabla %s.",nombre);
	free(path);
	return 0;
}

t_list *describe(char* nameTable){//PREGUNTAR, PORQUE 2 ATRIBUTOS, SI NAMETABLE ES NULL DEBERIA BASTAR
	t_list *tablas=list_create();
	if(nameTable==NULL){
		if(list_is_empty(directorio)){
			log_error(logger,"No hay ninguna tabla cargada en el sistema.");
		}else{
			//Recorrer el directorio de árboles de tablas
			//y descubrir cuales son las tablas que dispone el sistema.
			int cant=list_size(directorio);
			while(cant>0){
				char *tabla=list_get(directorio,cant-1);
				t_list *aux=describe(tabla);
				if(!list_is_empty(aux)){
					list_add_all(tablas,aux);
				}
				list_destroy(aux);
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

int drop(char* nameTable){
	//Verificar que la tabla exista en el file system.

	char *pathFolder=nivelUnaTabla(nameTable,0);
	char *path;
	if(folderExist(pathFolder)==0){
		//eliminar archivos binarios con sus respectivos bloques
		int cantBins=contarArchivos(nameTable, 0);
		int i=0;
		while(i<cantBins){
			path=nivelParticion(nameTable,i, 0);
			liberarParticion(path);
			free(path);
			i++;
		}
		//eliminar archivos temporales con sus respectivos bloques
		int cantDumps=contarArchivos(nameTable, 1);
		i=0;
		while(i<cantDumps){
			path=nivelParticion(nameTable,i, 1);
			liberarParticion(path);
			free(path);
			i++;
		}
		//eliminar archivos tempC con sus respectivos bloques
		int cantTmpc=contarArchivos(nameTable, 2);
		i=0;
		while(i<cantTmpc){
			path=nivelParticion(nameTable,i, 2);
			liberarParticion(path);
			free(path);
			i++;
		}
		 //eliminar archivo metadata
		path=nivelUnaTabla(nameTable,1);
		eliminarArchivo(path);
		free(path);
		//aumentar el semaforo contador

		//sacar la tabla del directorio
		int index=calcularIndexTabPorNombre(nameTable,directorio);			//	NO CALCULA BIEN EL INDEX
		char *tabla=list_remove(directorio,index);
		free(tabla);

		//Eliminar carpeta
		borrarCarpeta(pathFolder);
		free(pathFolder);
		log_info(logger,"Se ha eliminado la tabla %s",nameTable);

	}else{
		log_error(logger, "No se puede hacer el drop porque no existe la tabla %s.", nameTable);
		free(pathFolder);
		return 1;
	}
	return 0;
}
