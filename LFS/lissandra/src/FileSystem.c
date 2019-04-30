/*
 * FileSystem.c
 *
 *  Created on: 12 abr. 2019
 *      Author: utnso
 */

#include "FileSystem.h"

int folderExist(char* path){ //Verifica si existe la carpeta, si no existe devuelve 0
	struct stat st = {0};
	if (stat(path, &st) == -1){
		return 0;
	}else{
		return 1;
	}
}
