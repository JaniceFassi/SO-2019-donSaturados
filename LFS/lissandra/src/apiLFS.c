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
	char *path=pathFinal(param_nameTable,1,puntoMontaje);
	if(folderExist(path)==1){
		log_info(logger,"No existe la tabla %s", param_nameTable);
		free(path);
		return;
	}
	free(path);
	//	Obtener la metadata asociada a dicha tabla. PARA QUE?
	/*path=pathFinal(param_nameTable, 1,puntoMontaje);
	metaTabla *metadata= leerArchMetadata(path);
	free(path);*/

	/*Verificar si existe en memoria una lista de datos a dumpear.
	   De no existir, alocar dicha memoria.*/


	//El Timestamp es opcional. En caso que no este, se usará el valor actual del Epoch UNIX. ESTO SE HACE EN CONSOLA

	//Insertar en la memoria temporal del punto anterior una nueva entrada que contenga los datos enviados en la request.

	int ant=list_size(memtable);
	Registry *data = createRegistry(param_nameTable, param_key, param_value, param_timestamp);

	list_add(memtable, data);
	int tam= list_size(memtable);

	if(tam>ant){
		log_info(logger,"se agrego el registro a la lista");
	}

		char *rutaf=pathFinal(param_nameTable,2,puntoMontaje);
		strcat(rutaf,param_nameTable);
		strcat(rutaf,".txt");
		FILE* f =txt_open_for_append(rutaf);
		Registry *nodoEncontrado;		//MOMENTANEAMENTE SACO EL NODO PARA LUEGO ESCRIBIRLO EN UN ARCHIVO

		//PRIMERA APROXIMACION AL DUMP (CREACION DE ARCHIVOS TEMPORALES)
		nodoEncontrado= list_get(memtable,param_key);
		char *texto=malloc(255);
		//strcat(texto,prueba->timestamp);
		//strcat(texto,";");
		strcpy(texto,string_itoa(nodoEncontrado->key));
		strcat(texto,";");
		strcat(texto,nodoEncontrado->value);
		strcat(texto,"\n");

		txt_write_in_file(f,texto);

		txt_close_file(f);
		free(rutaf);
		//free(metadata->consistency);
		//free(metadata);
	//}
	//txt_close_file(metadata);
}

int selectS(char* nameTable , u_int16_t key, char *valor){
	//Verificar que la tabla exista en el file system.
	char *path=pathFinal(nameTable,1,puntoMontaje);
		if(folderExist(path)==1){
			log_info(logger,"No existe la tabla %s", nameTable);
			free(path);
			return 1;
		}else{
		//Obtener la metadata asociada a dicha tabla.
			/*free(path);
			path=pathFinal(nameTable, 3,puntoMontaje);
			metaTabla *metadata= leerArchMetadata(path);
			free(path);*/

		//Calcular cual es la partición que contiene dicho KEY.
		//int part=key % metadata->partitions;*/
		//Escanear la partición objetivo, todos los archivos temporales
		//y la memoria temporal de dicha tabla (si existe) buscando la key deseada.


		//Encontradas las entradas para dicha Key, se retorna el valor con el Timestamp más grande.

		//free(metadata->consistency);
		//free(metadata);
		//return 0;

		//}

	char *rutaf=pathFinal(nameTable,2,puntoMontaje);
	strcat(rutaf,nameTable);
	strcat(rutaf,".txt");
	char *texto=malloc(255);
	FILE *f=fopen(rutaf,"r");		//ABRO EL ARCHIVO Y MOMENTANEAMENTE LOS VALORES SE SEPARAN POR ;
	char c;

	while(feof(f)==0){
		c=fgetc(f);
		if(c==key){
			while(strcmp(&c,"\n")!=0){
				c=fgetc(f);
				strcat(texto,&c);
			}
			log_info(logger,texto);
			free(texto);
			free(rutaf);
			return 0;
		}
		else{
			while(strcmp(&c,"\n")!=0){
				c=fgetc(f);					//SI NO AUMENTA EL CONTADOR DE PALABRAS
			}
		}

	}
	log_info(logger,"No se encontro la key");	//SI TERMINO EL WHILE ES PORQUE NO ENCONTRO LA KEY Y LOGUEA*/
	free(rutaf);
	free(texto);
	return 1;
}
}

void create(char* nameTable, char* consistency , u_int16_t numPartition,long timeCompaction){
	//Verificar que la tabla no exista en el file system.
	//Por convención, una tabla existe si ya hay otra con el mismo nombre.
	//Para dichos nombres de las tablas siempre tomaremos sus valores en UPPERCASE (mayúsculas).
	//En caso que exista, se guardará el resultado en un archivo .log
	//y se retorna un error indicando dicho resultado.
	char *path=pathFinal(nameTable,1,puntoMontaje);
		if(folderExist(path)==0){
			log_info(logger, "Ya existe la Tabla %s",nameTable);
			perror("La tabla ya existe");
			free(path);
			return;
		}
	//Crear el directorio para dicha tabla.
	if(crearCarpeta(path)==1){
		log_info(logger,"ERROR AL CREAR LA TABLA %s",nameTable);
		free(path);
		return;
	}
	free(path);
	path=pathFinal(nameTable,3,puntoMontaje);
	//Crear el archivo Metadata asociado al mismo.
	//Grabar en dicho archivo los parámetros pasados por el request.
	crearArchMetadata(path,consistency,numPartition,timeCompaction);
	free(path);
	path=pathFinal(nameTable,2,puntoMontaje);
	//Crear los archivos binarios asociados a cada partición de la tabla y
	if(crearParticiones(path,numPartition)==1){
		log_info(logger,"ERROR AL CREAR LAS PARTICIONES");
		free(path);
		return;
	}
	//asignar a cada uno un bloque

	free(path);
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
		char *path=pathFinal(nameTable,1,puntoMontaje);
		if(folderExist(path)==0){
			free(path);
			path=pathFinal(nameTable,3,puntoMontaje);
			//Leer el archivo Metadata de dicha tabla.
			metaTabla *metadata= leerArchMetadata(path);
			//Retornar el contenido del archivo.
			free(path);

		}else{
			log_info(logger, "No existe esa Tabla");
			free(path);
			return 1;
		}

	}
	return 0;
}

void drop(char* nameTable){
	//Verificar que la tabla exista en el file system.

	char *path=pathFinal(nameTable,1,puntoMontaje);
	if(folderExist(path)==0){
		//Eliminar directorio y todos los archivos de dicha tabla.
		borrarCarpeta(path);

	}else{
		log_info(logger, "No existe esa Tabla");
	}
	free(path);
}
