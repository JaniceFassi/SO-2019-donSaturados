
#include "apiLFS.h"

//API

int insert(char *param_nameTable, u_int16_t param_key, char *param_value, long param_timestamp){
	//RETARDO
	sem_wait(inotifyRetardo);
	int retardo=configLissandra->retardo*1000;
	sem_post(inotifyRetardo);
	usleep(retardo);
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
		log_error(logger,"No se puede hacer el insert porque el valor excede el tamanio permitido.");
		return 1;
	}
	/*Verificar si existe en memoria una lista de datos a dumpear.
	   De no existir, alocar dicha memoria.*/
	//WAIT DE MEMTABLE
	sem_wait(sem_dump);
	sem_wait(criticaMemtable);
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
	//SIGNAL DE MEMTABLE
	sem_post(criticaMemtable);
	sem_post(sem_dump);
	log_info(logger,"Se ha insertado el registro con key %i y valor %s en la memtable.",param_key,param_value);
	return 0;
}

char *lSelect(char *nameTable, u_int16_t key){
	sem_wait(inotifyRetardo);
	int retardo=configLissandra->retardo*1000;
	sem_post(inotifyRetardo);
	usleep(retardo);
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

	//Escanear la memtable
	t_list *aux;
	log_info(logger,"Se escanea la memtable.");
	sem_wait(criticaMemtable);
	if(!list_is_empty(memtable)){
		Tabla *encontrada= find_tabla_by_name_in(nameTable, memtable);
		if(encontrada!=NULL){
			aux=filtrearPorKey(encontrada->registros,key);
		}else{
			aux=list_create();
		}
	}else{
		aux=list_create();
	}
	sem_post(criticaMemtable);
	//log_info(logger,"se leyeron los datos de la memtable");
	//Escanear todos los archivos temporales (modo 1)
	int cantDumps=contarArchivos(nameTable, 1);
	if(cantDumps>0){
		log_info(logger,"Se escanea los temporales.");
	}else{
		log_info(logger,"No hay temporales para escanear.");
	}
	int i=0;
	while(i<cantDumps){
		escanearArchivo(nameTable,i, 1, obtenidos);
		//log_info(logger,"se logro leer los temporales");
		i++;
	}
	//Escanear los .tmpc si es necesario (modo 2)
	int cantTmpc=contarArchivos(nameTable ,2);
	if(cantTmpc>0){
		log_info(logger,"Se escanea los temporales de compactacion.");
	}else{
		log_info(logger,"No hay temporales de compactacion para escanear.");
	}
	i=0;
	while(i<cantTmpc){
		escanearArchivo(nameTable,i, 2, obtenidos);
		//log_info(logger,"se pudo leer los tempC");
		i++;
	}

	//Obtener la metadata asociada a dicha tabla.

	//log_info(logger,"quiere leer la META");
	metaTabla *metadata= leerMetadataTabla(nameTable);

	//log_info(logger,"se pudo leer la META");
	//Calcular cual es la partición que contiene dicho KEY.
	int part=key % metadata->partitions;
	log_info(logger, "La key %i esta contenida en la particion %i.",key, part);
	//Escanear la partición objetivo (modo 0)
	log_info(logger,"Se escanea la particion objetivo.");
	escanearArchivo(nameTable, part, 0,obtenidos);
	//log_info(logger,"pudo leer el bin");
	if(list_size(obtenidos)!=0){
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
			log_info(logger,"No se ha encontrado el valor con key %i de la tabla %s.",key,nameTable);
		}
	}else{
		log_info(logger,"No se ha encontrado el valor con key %i de la tabla %s.",key,nameTable);
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
	sem_wait(inotifyRetardo);
	int retardo=configLissandra->retardo*1000;
	sem_post(inotifyRetardo);
	usleep(retardo);
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
	//if(hayXBloquesLibres(numPartition)){
	if(cantBloqGlobal>=numPartition){
		sem_wait(criticaCantBloques);
		cantBloqGlobal-=numPartition;
		sem_post(criticaCantBloques);
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
			sem_wait(criticaCantBloques);
			cantBloqGlobal+=numPartition;
			sem_post(criticaCantBloques);
			return 1;
		}
		borrarMetadataTabla(tabla);
	}else{
		log_error(logger,"No hay %i bloques libres.\n",numPartition);
		free(path);
		free(nombre);
		return 1;
	}
	log_info(logger,"Se ha creado la tabla %s.",nombre);
	free(path);
	Sdirectorio *uno=malloc(sizeof(Sdirectorio));
	uno->nombre=string_duplicate(nombre);
	uno->time_compact=timeCompaction;
	uno->terminar=1;
	semaforosTabla(uno);
	sem_wait(criticaDirectorio);
	list_add(directorioP,uno);
	pthread_create(&uno->hilo, NULL, &compactar,uno);
	sem_post(criticaDirectorio);
	free(nombre);
	return 0;
}

t_list *describe(char* nameTable){//PREGUNTAR, PORQUE 2 ATRIBUTOS, SI NAMETABLE ES NULL DEBERIA BASTAR
	sem_wait(inotifyRetardo);
	int retardo=configLissandra->retardo*1000;
	sem_post(inotifyRetardo);
	usleep(retardo);
	t_list *tablas=list_create();
	if(nameTable==NULL){
		if(list_is_empty(directorioP)){
			log_info(logger,"No hay ninguna tabla cargada en el sistema.");
		}else{
			//Recorrer el directorio de árboles de tablas
			//y descubrir cuales son las tablas que dispone el sistema.
			int cant=list_size(directorioP);
			while(cant>0){
				Sdirectorio *tabla=list_get(directorioP,cant-1);
				t_list *aux=describe(tabla->nombre);
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
			metaTabla *metadata=leerMetadataTabla(nameTable);
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
	sem_wait(inotifyRetardo);
	int retardo=configLissandra->retardo*1000;
	sem_post(inotifyRetardo);
	usleep(retardo);
	//Verificar que la tabla exista en el file system.
	char *pathFolder=nivelUnaTabla(nameTable,0);
	char *path;
	if(folderExist(pathFolder)==0){
		Sdirectorio *tabDirectorio=obtenerUnaTabDirectorio(nameTable);
		cerrarHiloCompactor(tabDirectorio);
		//eliminar archivos binarios con sus respectivos bloques
		int cantBins=contarArchivos(nameTable, 0);
		int i=0;
		sem_wait(&tabDirectorio->semaforoBIN);
		while(i<cantBins){
			path=nivelParticion(nameTable,i, 0);
			liberarParticion(path);
			log_info(logger,"Se elimino el %i.bin de %s.",i,nameTable);
			free(path);
			i++;
		}
		sem_post(&tabDirectorio->semaforoBIN);
		//eliminar archivos temporales con sus respectivos bloques
		int cantDumps=contarArchivos(nameTable, 1);
		i=0;
		sem_wait(&tabDirectorio->semaforoTMP);
		while(i<cantDumps){
			path=nivelParticion(nameTable,i, 1);
			liberarParticion(path);
			log_info(logger,"Se elimino el %i.tmp de %s.",i,nameTable);
			free(path);
			i++;
		}
		sem_post(&tabDirectorio->semaforoTMP);
		//eliminar archivos tempC con sus respectivos bloques
		int cantTmpc=contarArchivos(nameTable, 2);
		i=0;
		sem_wait(&tabDirectorio->semaforoTMPC);
		while(i<cantTmpc){
			path=nivelParticion(nameTable,i, 2);
			liberarParticion(path);
			log_info(logger,"Se elimino el %i.tmpc de %s.",i,nameTable);
			free(path);
			i++;
		}
		sem_post(&tabDirectorio->semaforoTMPC);

		//eliminar archivo metadata
		path=nivelUnaTabla(nameTable,1);
		sem_wait(&tabDirectorio->semaforoMeta);
		eliminarArchivo(path);
		log_info(logger,"Se elimino la metadata de %s.",nameTable);
		sem_post(&tabDirectorio->semaforoMeta);

		free(path);
		//sacar la tabla del directorio
		sem_wait(criticaDirectorio);
		bool encontrar(Sdirectorio* compara){
			return string_equals_ignore_case(compara->nombre,nameTable);
		}

		Sdirectorio *nuevo=list_remove_by_condition(directorioP,(void *)encontrar);
		//log_info(logger,"se removio %s",nuevo->nombre);
		liberarTabDirectorio(nuevo);
		sem_post(criticaDirectorio);
		//Eliminar carpeta
		if(borrarCarpeta(pathFolder)){
			log_info(logger,"no se pudo eliminar la carpeta de %s",nameTable);
		}
		free(pathFolder);
		log_info(logger,"Se ha eliminado la tabla %s",nameTable);
	}else{
		log_error(logger, "No se puede hacer el drop porque no existe la tabla %s.", nameTable);
		free(pathFolder);
		return 1;
	}
	return 0;
}
