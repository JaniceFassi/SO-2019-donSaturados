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
	if(folderExist(param_nameTable,1)==1){
		log_info(logger,"No existe la tabla %s", param_nameTable);
		return;
	}
	//	Obtener la metadata asociada a dicha tabla. PARA QUE?
	char *path=pathFinal(param_nameTable, 1);
	FILE *metadata = txt_open_for_append(path);

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

		char *rutaf=malloc(255);
		strcpy(rutaf,"/home/utnso/");
		strcat(rutaf,param_nameTable);
		strcat(rutaf,".txt");
		FILE* f =txt_open_for_append(rutaf);
		Registry *prueba;					//MOMENTANEAMENTE SACO EL NODO PARA LUEGO ESCRIBIRLO EN UN ARCHIVO

		prueba= list_get(memtable,param_key);
		strcat(prueba->name,";");
		txt_write_in_file(f,prueba->name);

		txt_close_file(f);
		free(rutaf);
	//}
	//txt_close_file(metadata);
}

int selectS(char* nameTable , u_int16_t key, char *valor){
	//Verificar que la tabla exista en el file system.
	if(folderExist(nameTable,1)==0){
		log_info(logger,"No existe la tabla %s",nameTable);
		return 1;
	}else{
		//Obtener la metadata asociada a dicha tabla.
		char *path=pathFinal(nameTable,1);
		FILE *metadata = txt_open_for_append(path);

		//Calcular cual es la partición que contiene dicho KEY.
		char* partition=config_get_string_value(metadata, "PARTITIONS");
		int part=key % atoi(partition);
		//Escanear la partición objetivo, todos los archivos temporales
		//y la memoria temporal de dicha tabla (si existe) buscando la key deseada.


		//Encontradas las entradas para dicha Key, se retorna el valor con el Timestamp más grande.
		txt_close_file(metadata);
		return 0;

		}

	/*char *rutaf=malloc(255);
	strcpy(rutaf,"/home/utnso/");
	strcat(rutaf,nameTable);
	strcat(rutaf,".txt");
	char *texto=malloc(255);
	char c;
	FILE *f=fopen(rutaf,"r");		//ABRO EL ARCHIVO Y MOMENTANEAMENTE LOS VALORES SE SEPARAN POR ; HAY QUE ENCONTRAR EL VALOR DE LA ; NUMERO (KEY).
	int count=0;
	while(EOF){
		while(strcmp(c,";")!=0){		//GUARDO EL TEXTO HASTA EL ;
			c=getc(f);
			strcat(texto,&c);
		}
		if(count==key){				//SI TUVE LA CANTIDAD DE PALABRAS QUE NECESITO, LOGUEA
			log_info(logger,texto);
			free(texto);
			free(rutaf);
			return 0;
		}
		else{
			count++;				//SI NO AUMENTA EL CONTADOR DE PALABRAS
		}
	}
	log_info(logger,"No se encontro la key");	//SI TERMINO EL WHILE ES PORQUE NO ENCONTRO LA KEY Y LOGUEA
	free(rutaf);
	free(texto);
	return -1;*/
}

void create(char* nameTable, char* consistency , u_int16_t numPartition,long timeCompaction){
	//Verificar que la tabla no exista en el file system.
	//Por convención, una tabla existe si ya hay otra con el mismo nombre.
	//Para dichos nombres de las tablas siempre tomaremos sus valores en UPPERCASE (mayúsculas).
	//En caso que exista, se guardará el resultado en un archivo .log
	//y se retorna un error indicando dicho resultado.
	if(folderExist(nameTable,1)==0){
		log_info(logger, "Ya existe la Tabla %s",nameTable);
		perror("La tabla ya existe");
		return;
	//Crear el directorio para dicha tabla.
	if(crearCarpeta(nameTable,1)==1){
		log_info(logger,"ERROR AL CREAR LA TABLA %s",nameTable);
		return;
	}
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
		if(folderExist(nameTable,1)==0){
			//Leer el archivo Metadata de dicha tabla.

			//Retornar el contenido del archivo.
		}else{
			log_info(logger, "No existe esa Tabla");
			return 1;
		}


	}
	return 0;
}

void drop(char* nameTable){
	//Verificar que la tabla exista en el file system.

	if(folderExist(nameTable,1)==0){
		//Eliminar directorio y todos los archivos de dicha tabla.
		borrarCarpeta(nameTable,1);
	}else{
		log_info(logger, "No existe esa Tabla");
	}
}
