
/*
 * FileSystem.c
 *
 *  Created on: 12 abr. 2019
 *      Author: utnso
 */
#include "TADs.h"


/************************************************************************************************/
//FUNCIONES DE CONCATENAR

//A REEMPLAZAR
char *pathFinal(char *nombre, int principal){
	//0 ES LA CARP PRINCIPAL, 1 ES LA CARP DE LAS TABLAS, 2 PATH DE ARCH, 3 METADATA
	int base=string_length(configLissandra->puntoMontaje)+string_length(nombre)+1;
	char *pathF=malloc(base);
	strcpy(pathF,configLissandra->puntoMontaje);

	if(principal==0){
		strcat(pathF,nombre);
		return pathF;				// /home/utnso/tp-2019-1c-donSaturados/LFS/nombre
	}

	char tab[]="TABLAS/";
	base+=string_length(tab)+1;
	pathF=realloc(pathF,base);
	strcat(pathF,tab);
	strcat(pathF,nombre);

	if(principal==1){
		return pathF;				// /home/utnso/tp-2019-1c-donSaturados/LFS/Tablas/nombre
	}

	char barra[]="/";
	base +=string_length(barra)+1;
	pathF=realloc(pathF,base);
	strcat(pathF,barra);

	if(principal==2){
		return pathF;				// /home/utnso/tp-2019-1c-donSaturados/LFS/Tablas/nombre/
	}

	char metadata[]="METADATA";
	base += string_length(metadata)+1;
	pathF=realloc(pathF,base);

	if(principal==3){
		strcat(pathF,metadata);
		return pathF;				// /home/utnso/tp-2019-1c-donSaturados/LFS/Tablas/nombre/METADATA
	}
	free(pathF);
	return NULL;
}

//A REEMPLAZAR
char *concatExtencion(char *name,int particion, int tipo){//0 es bin, si es 1 es temp
	char *pathF=pathFinal(name,2);
	int base=strlen(pathF)+1;
	char *part=string_itoa(particion);
	base+=strlen(part)+1;
	pathF=realloc(pathF,base);
	strcat(pathF,part);
	free(part);
	if(tipo==0){
		char *extencion= malloc(5);
		strcpy(extencion, ".bin");
		base+=strlen(extencion)+1;
		pathF=realloc(pathF,base);
		strcat(pathF,extencion);
		free(extencion);

	}else{
		char *extencion= malloc(6);
		strcpy(extencion, ".temp");
		base+=strlen(extencion)+1;
		pathF=realloc(pathF,base);
		strcat(pathF,extencion);
		free(extencion);
	}
	return pathF;
}
char *extension(char *path,int modo){				//0 .bin, 1 .tmp, 2 .tmpc
	char *punto=malloc(2);
	strcpy(punto,".");
	path=realloc(path,strlen(path)+strlen(punto)+1);
		strcat(path,punto);

		if(modo==0){
			char *bin=malloc(4);
			strcpy(bin,"bin");
			path=realloc(path,strlen(path)+strlen(bin)+1);
			strcat(path,bin);
			free(bin);
		}
		if(modo==1){
			char *tmp=malloc(4);
			strcpy(tmp,"tmp");
			path=realloc(path,strlen(path)+strlen(tmp)+1);
			strcat(path,tmp);
			free(tmp);
		}
		if(modo==2){
			char *tmpc=malloc(5);
			strcpy(tmpc,"tmpc");
			path=realloc(path,strlen(path)+strlen(tmpc)+1);
			strcat(path,tmpc);
			free(tmpc);
		}
		free(punto);
		return path;
}

Registry *desconcatParaArch(char *linea){
	char **subString=string_n_split(linea,3,";");
	int key=atoi(subString[1]);
	long timestamp=atol(subString[0]);
	Registry *nuevo=createRegistry(key,subString[2],timestamp);
	return nuevo;
}
char *array_A_String(char **array,int cantBloques){
	char *casteo=malloc(2);
	strcpy(casteo,"[");
	int i=0;
	while(i<cantBloques){
		casteo=realloc(casteo,strlen(casteo)+strlen(array[i])+1);
		strcat(casteo,array[i]);
		if(i!=cantBloques-1){
			casteo=realloc(casteo,strlen(casteo)+2);
			strcat(casteo,",");
		}
		i++;
	}
	casteo=realloc(casteo,strlen(casteo)+2);
	strcat(casteo,"]");
	return casteo;
}

char *concatRegistro(Registry *registro){
	char *keys=string_itoa(registro->key);
	char *time=string_from_format("%ld",registro->timestamp);
	char *linea=malloc(strlen(time)+1);
	strcpy(linea,time);
	linea=ponerSeparador(linea);
	linea=realloc(linea,strlen(linea)+strlen(keys)+1);
	strcat(linea,keys);
	linea=ponerSeparador(linea);
	linea=realloc(linea,strlen(linea)+strlen(registro->value)+1);
	strcat(linea, registro->value);
	return linea;
}

char *concatParaArchivo(long timestamp,int key,char *value,int opc){
	//0 para Escribir, 1 para Agregar

	char *separador=";";
	char *salto="\n";
	int base;

	char *keys=string_itoa(key);
	char *time=string_from_format("%ld",timestamp);
	char *linea;

	if(opc==0){
		base=strlen(time)+1;
		linea=malloc(base);
		strcpy(linea,time);

	}else{
		base=strlen(salto)+1;
		char *linea=malloc(base);
		strcpy(linea,salto);
		base+=strlen(time)+1;
		linea=realloc(linea,base);
		string_append(&linea,time);
	}
	free(time);

	base +=strlen(separador)+1;
	linea=realloc(linea,base);
	string_append(&linea,separador);

	base +=strlen(keys)+1;
	linea=realloc(linea,base);
	string_append(&linea,keys);
	free(keys);

	base +=strlen(separador)+1;
	linea=realloc(linea,base);
	string_append(&linea,separador);

	base +=strlen(value)+1;
	linea=realloc(linea,base);
	string_append(&linea,value);

	base +=strlen(salto)+1;
	linea=realloc(linea,base);
	string_append(&linea,salto);

	return linea;
}

char *ponerBarra(char *linea){
	char *barra=malloc(2);
	strcpy(barra,"/");
	linea=realloc(linea,strlen(linea)+strlen(barra)+1);
	strcat(linea,barra);
	free(barra);
	return linea;
}

char *ponerSeparador(char *linea){
	char *separador=malloc(2);
	strcpy(separador,";");
	linea=realloc(linea,strlen(linea)+strlen(separador)+1);
	strcat(linea,separador);
	free(separador);
	return linea;
}

char *obtenerMontaje(){
	char *montaje=malloc(strlen(raizDirectorio)+1);
	strcpy(montaje,raizDirectorio);
	montaje=realloc(montaje,strlen(montaje)+strlen(configLissandra->puntoMontaje)+1);
	strcat(montaje,configLissandra->puntoMontaje);
	return montaje;
}

char *nivelMetadata(int modo){	//0 metadata carpeta, 1 metadata.bin, 2 metadata.bitmap
	char *path=obtenerMontaje();
	char *metadata=malloc(9);
	strcpy(metadata,"METADATA");
	path=realloc(path,strlen(path)+strlen(metadata)+1);
	strcat(path,metadata);

	if(modo==1){
		path=ponerBarra(path);
		path=realloc(path,strlen(path)+strlen(metadata)+1);
		strcat(path,metadata);
		path=extension(path,0);
	}

	if(modo==2){
		path=ponerBarra(path);
		char *bitmap=malloc(7);
		strcpy(bitmap,"BITMAP");
		path=realloc(path,strlen(path)+strlen(bitmap)+1);
		strcat(path,bitmap);
		path=extension(path,0);
		free(bitmap);
	}
	free(metadata);
	return path;
}
char *nivelTablas(){
	char *path=obtenerMontaje();
	char *tablas=malloc(7);
	strcpy(tablas,"TABLAS");
	path=realloc(path,strlen(path)+strlen(tablas)+1);
	strcat(path,tablas);
	path=ponerBarra(path);
	free(tablas);
	return path;
}

char *nivelBloques(){
	char *path=obtenerMontaje();
	char *bloques=malloc(8);
	strcpy(bloques,"BLOQUES");
	path=realloc(path,strlen(path)+strlen(bloques)+1);
	strcat(path,bloques);
	path=ponerBarra(path);
	free(bloques);
	return path;
}

char *rutaBloqueNro(int nroBloque){
	char *path=nivelBloques();
	char *nro=string_itoa(nroBloque);
	path=realloc(path,strlen(path)+strlen(nro)+1);
	strcat(path,nro);
	path=extension(path,0);
	free(nro);
	return path;
}
char *nivelUnaTabla(char *nombre, int modo){			//0 carpeta tabla, 1 metaTabla
	char *path=nivelTablas();
	path=realloc(path,strlen(path)+strlen(nombre)+1);
	strcat(path,nombre);										//montaje/TABLAS/TABLA/
	path=ponerBarra(path);

	if(modo==1){
		char *metadata=malloc(9);
		strcpy(metadata,"METADATA");
		path=realloc(path,strlen(path)+strlen(metadata)+1);		//montaje/TABLAS/TABLA/METADATA.bin
		strcat(path,metadata);
		path=extension(path,0);
		free(metadata);
	}
	return path;
}

char *nivelParticion(char *tabla, int particion, int modo){		//montaje/TABLAS/TABLA/part.ext		modo->ext 0 .bin, 1 .tmp, 2 .tmpc
	char *path=nivelUnaTabla(tabla,2);
	char *part=string_itoa(particion);
	path=realloc(path,strlen(path)+strlen(part)+1);
	strcat(path,part);
	path=extension(path,modo);
	free(part);
	return path;
}

/****************************************************************************************************/
//FUNCIONES DE CARPETAS

int crearCarpeta(char* path){//CREA LA CARPETA
	if(mkdir(path,0777)<0){
		printf("No se pudo crear la Carpeta\n");
			return 1;
	}
	return 0;
}

int folderExist(char* path){ //Verifica si existe la carpeta, si no existe devuelve 1

	struct stat st = {0};
	if (stat(path, &st) == -1){
		//printf("No existe la Carpeta\n");
		return 1;
	}
	return 0;
}

int borrarCarpeta(char *path){//BORRA LA CARPETA

	if(rmdir(path)<0){
		printf("No se pudo borrar la Carpeta\n");
		return 1;
	}
	return 0;
}

/***********************************************************************************************/
//FUNCIONES DE ARCHIVOS

int crearParticiones(metaTabla *tabla){
	FILE* arch; //BINARIO
	int cant=tabla->partitions;
	char *path;
	while(cant>0){
		path=nivelParticion(tabla->nombre,cant-1, 0);
		int bloque=obtenerBloqueVacio();
		if((arch= fopen(path,"wb"))<0){
			log_info(logger,"Error al crear la particion %i de la tabla %s.\n",tabla->partitions,tabla->nombre);
			 desocuparBloque(bloque);
			return 1;
		}
		char **arrayBlock=malloc(sizeof(int));
		arrayBlock[0]=malloc(strlen(string_itoa(bloque))+1);
		strcpy(arrayBlock[0],string_itoa(bloque));
		if(crearMetaArchivo(path, 0,arrayBlock ,1)!=0){
			//mensaje de error
		}
		//crearMetaArchivo(path,bloque);
		fclose(arch);
		free(path);
		free(arrayBlock[0]);
		free(arrayBlock);
		//liberarSubstrings(arrayBlock);
		cant --;
	}
	return 0;
}

int crearMetaArchivo(char *path, int size, char **bloques, int cantBloques){
	char *bloqCasteado=array_A_String(bloques,cantBloques);
	FILE *f=fopen(path,"wb");
	if(f!=NULL){
		fclose(f);
	}else{
		return 1;
	}
	t_config *metaArchs=config_create(path);
	char *tamanio=string_itoa(size);
	config_set_value(metaArchs,"SIZE",tamanio);
	config_set_value(metaArchs,"BLOCKS",bloqCasteado);
	config_save(metaArchs);
	config_destroy(metaArchs);
	free(tamanio);
	free(bloqCasteado);
	return 0;
}

/*void crearMetaArchivo(char *path, int nrobloque){						//Desde cero, con size 0 y un solo bloque. Podria sacarse
	char **blocks=malloc(sizeof(int));
	blocks[0]=malloc(strlen(string_itoa(nrobloque))+1);
	strcpy(blocks[0],string_itoa(nrobloque));
	char *bloque=array_A_String(blocks,1);
	t_config *metaArchs=config_create(path);
	char* size = string_itoa(0);
	config_set_value(metaArchs,"SIZE",size);
	config_set_value(metaArchs,"BLOCKS",bloque);
	config_save(metaArchs);
	config_destroy(metaArchs);
	free(size);
	liberarSubstrings(blocks);
	free(bloque);
}*/

void liberarSubstrings(char **liberar){
	int i=1;
	while(liberar[i-1]!=NULL){
		free(liberar[i-1]);
		//log_info(logger, "%s",liberar[i]);
		i++;
	}
	free(liberar);
}
void borrarMetaArch(metaArch *archivo){
	liberarSubstrings(archivo->bloques);
	free(archivo);
}
metaArch *leerMetaArch(char *path){
	metaArch *nuevo=malloc(sizeof(metaArch));
	t_config *metaArchs=config_create(path);
	nuevo->size=config_get_int_value(metaArchs,"SIZE");
	nuevo->bloques=config_get_array_value(metaArchs,"BLOCKS");
	config_destroy(metaArchs);
	return nuevo;
}
metaTabla *crearMetadataTabla(char* nombre, char* consistency , u_int16_t numPartition,long timeCompaction){
	char *path=nivelUnaTabla(nombre,1);
	metaTabla *nuevo=malloc(sizeof(metaTabla));
	nuevo->compaction_time=timeCompaction;
	nuevo->consistency= malloc(strlen(consistency)+1);
	strcpy(nuevo->consistency,consistency);
	nuevo->partitions= numPartition;
	nuevo->nombre=malloc(strlen(nombre)+1);
	strcpy(nuevo->nombre,nombre);
	FILE* metadata= fopen(path,"wb"); //BINARIO
	fclose(metadata);
	t_config *metaTab=config_create(path);
	config_set_value(metaTab,"CONSISTENCY",nuevo->consistency);
	char* cantParticiones = string_itoa(nuevo->partitions);
	config_set_value(metaTab,"PARTITIONS",cantParticiones);
	char* tiempoCompact = string_from_format("%ld",nuevo->compaction_time);
	config_set_value(metaTab,"COMPACTION_TIME",tiempoCompact);
	config_save(metaTab);
	config_destroy(metaTab);

	free(cantParticiones);
	free(tiempoCompact);
	free(path);
	return nuevo;
}

metaTabla *leerMetadataTabla(char *nombre){
	char *path=nivelUnaTabla(nombre,1);
	t_config *metaTab=config_create(path);
	metaTabla *nuevo=malloc(sizeof(metaTabla));
	nuevo->compaction_time=config_get_long_value(metaTab, "COMPACTION_TIME");
	char *aux=config_get_string_value(metaTab, "CONSISTENCY");
	nuevo->consistency=malloc(string_length(aux)+1);
	strcpy(nuevo->consistency,aux);
	nuevo->partitions= config_get_int_value(metaTab, "PARTITIONS");
	nuevo->nombre=malloc(strlen(nombre)+1);
	strcpy(nuevo->nombre,nombre);
	config_destroy(metaTab);
	return nuevo;
}
void borrarMetadataTabla(metaTabla *metadata){
	free(metadata->consistency);
	free(metadata->nombre);
	free(metadata);
}

void oldCrearMetaLFS(u_int16_t size,u_int16_t cantBloques,char *magicNumber){
	char *path=nivelMetadata(1);
	FILE *arch=fopen(path,"wb");
	fclose(arch);
	metaLFS = malloc(sizeof(metaFileSystem));
	metaLFS->magicNumber=malloc(strlen(magicNumber)+1);
	strcpy(metaLFS->magicNumber,magicNumber);
	metaLFS->cantBloques=cantBloques;
	metaLFS->tamBloques=size;
	t_config *config = config_create(path);
	config_set_value(config, "BLOQUES", string_itoa(metaLFS->cantBloques));
	config_set_value(config, "TAMANIO", string_itoa(metaLFS->tamBloques));
	config_set_value(config, "MAGIC_NUMBER", metaLFS->magicNumber);
	config_save(config);
	config_destroy(config);
	free(path);
}

void crearMetaLFS(){
	char *path=nivelMetadata(1);
	FILE *arch=fopen(path,"wb");
	fclose(arch);
	metaLFS =malloc(sizeof(metaFileSystem));
	printf("Ingrese la cantidad de bloques: ");
	scanf("%i",&metaLFS->cantBloques);
	printf("Ingrese el tamaño de los bloques: ");
	scanf("%i",&metaLFS->tamBloques);
	printf("Ingrese el magic Number 'Lissandra' : ");
	char *magic=malloc(15);
	scanf("%s",magic);
	metaLFS->magicNumber=malloc(strlen(magic)+1);
	strcpy(metaLFS->magicNumber,magic);
	free(magic);
	t_config *config = config_create(path);
	char *bloq=string_itoa(metaLFS->cantBloques);
	char *tam=string_itoa(metaLFS->tamBloques);
	config_set_value(config, "BLOQUES", bloq);
	config_set_value(config, "TAMANIO", tam);
	config_set_value(config, "MAGIC_NUMBER", metaLFS->magicNumber);
	config_save(config);
	config_destroy(config);
	free(bloq);
	free(tam);
	free(path);
}

void leerMetaLFS(){
	char *path=nivelMetadata(1);
	t_config *metadataLFS=config_create(path);
	metaLFS=malloc(sizeof(metaFileSystem));
	metaLFS->tamBloques=config_get_int_value(metadataLFS, "TAMANIO");
	char *magicN=config_get_string_value(metadataLFS, "MAGIC_NUMBER");
	metaLFS->magicNumber=malloc(string_length(magicN)+1);
	strcpy(metaLFS->magicNumber,magicN);
	metaLFS->cantBloques= config_get_int_value(metadataLFS, "BLOQUES");
	config_destroy(metadataLFS);
	free(path);
}

void borrarMetaLFS(){
	free(metaLFS->magicNumber);
	free(metaLFS);
}

void estructurarConfig(){							//Lee el config y crea una estructura con esos datos
	t_config *config = init_config();				//Crea una estructura datosConfig en configLissandra, variable global
	configLissandra=malloc(sizeof(datosConfig));
	char *aux= config_get_string_value(config,"PUNTO_MONTAJE");
	configLissandra->puntoMontaje=malloc(strlen(aux)+1);
	strcpy(configLissandra->puntoMontaje,aux);

	configLissandra->tiempoDump=config_get_long_value(config,"TIEMPO_DUMP");
	char *aux2=config_get_string_value(config, "IP");
	configLissandra->Ip=malloc(strlen(aux2)+1);
	strcpy(configLissandra->Ip,aux2);
	configLissandra->puerto= config_get_int_value(config, "PORT");
	configLissandra->id= config_get_int_value(config, "ID");
	configLissandra->idEsperado= config_get_int_value(config, "ID_ESPERADO");
	configLissandra->retardo= config_get_int_value(config, "RETARDO");
	configLissandra->tamValue= config_get_int_value(config, "TAMVALUE");
	config_destroy(config);
}

void borrarDatosConfig(){
	free(configLissandra->Ip);
	free(configLissandra->puntoMontaje);
	free(configLissandra);
}

void crearConfig(){
	FILE *config=fopen(pathInicial,"wb");
	fclose(config);

	configLissandra=malloc(sizeof(datosConfig));

	printf("Ingrese el punto de montaje correspendiente, sin el home/utnso: ");
	char *path=malloc(100);
	scanf("%s",path);
	configLissandra->puntoMontaje=malloc(strlen(path)+1);
	strcpy(configLissandra->puntoMontaje,path);
	free(path);

	printf("\nIngrese el tamaño maximo del value: ");
	scanf("%i",&configLissandra->tamValue);

	printf("\nIngrese el ID de Lissandra: ");
	scanf("%i",&configLissandra->id);

	printf("\nIngrese el ID de Memoria: ");
	scanf("%i",&configLissandra->idEsperado);

	printf("Ingrese la IP de Lissandra: ");
	char *ip=malloc(25);
	scanf("%s",ip);
	configLissandra->Ip=malloc(strlen(ip)+1);
	strcpy(configLissandra->Ip,ip);
	free(ip);

	printf("\nIngrese el puerto de escucha: ");
	scanf("%i",&configLissandra->puerto);

	printf("\nIngrese el tiempo para el dump: ");
	scanf("%i",&configLissandra->tiempoDump);

	printf("\nIngrese el tiempo de retardo: ");
	scanf("%i",&configLissandra->retardo);

	t_config *lissConfig=config_create(pathInicial);

	config_set_value(lissConfig,"PUNTO_MONTAJE",configLissandra->puntoMontaje);

	char *tamanio=string_itoa(configLissandra->tamValue);
	config_set_value(lissConfig,"TAMVALUE",tamanio);

	char *id=string_itoa(configLissandra->id);
	config_set_value(lissConfig,"ID",id);

	char *idEsperado=string_itoa(configLissandra->idEsperado);
	config_set_value(lissConfig,"ID_ESPERADO",idEsperado);

	config_set_value(lissConfig,"IP",configLissandra->Ip);

	char *puerto=string_itoa(configLissandra->puerto);
	config_set_value(lissConfig,"PORT",puerto);

	char *tempDump=string_itoa(configLissandra->tiempoDump);
	config_set_value(lissConfig,"TIEMPO_DUMP",tempDump);

	char *tempRetar=string_itoa(configLissandra->retardo);
	config_set_value(lissConfig,"RETARDO",tempRetar);

	config_save(lissConfig);
	config_destroy(lissConfig);

	free(tamanio);
	free(id);
	free(idEsperado);
	free(puerto);
	free(tempDump);
	free(tempRetar);
}

// NUEVAS FUNCIONES DE ARCHIVOS *******************************************************************
void escribirBloque(char *buffer,char **bloques){
	int nroArray=0;
	while(strlen(buffer)>metaLFS->tamBloques){
		char *escribir=string_substring_until(buffer,metaLFS->tamBloques);
		if(bloques[nroArray]!=NULL){
			int nroBloq=atoi(bloques[nroArray]);
			char *pathB=rutaBloqueNro(nroBloq);
			//escribir el archivo
			escribirArchB(pathB,escribir);
			free(pathB);
		}else{
			//error
		}
		char *auxilar=string_substring_from(buffer,metaLFS->tamBloques);
		free(buffer);
		buffer=malloc(strlen(auxilar)+1);
		strcpy(buffer,auxilar);
		free(auxilar);
		nroArray++;
	}
	if(strlen(buffer)>0){
		if(bloques[nroArray]!=NULL){
			int nroBloq=atoi(bloques[nroArray]);
			char *pathB=rutaBloqueNro(nroBloq);
			//escribir el archivo
			escribirArchB(pathB,buffer);
			free(pathB);
		}else{
			//error
		}
	}
	free(buffer);
}

void escribirArchB(char *path,char *buffer){
	FILE *bloque=fopen(path,"wb");
	fwrite(path,sizeof(char),strlen(path)+1,bloque);
	fflush(bloque);
	fclose(bloque);
}

t_list *leerBloques(char**bloques,int offset){
	t_list *registrosLeidos=list_create();
	int blok=0;
	char *leidoTotal;
	while(offset>metaLFS->tamBloques){
		if(bloques[blok]!=NULL){
			int nroBloq=atoi(bloques[blok]);
			char *pathB=rutaBloqueNro(nroBloq);
			char *leido=leerArchBinario(pathB,metaLFS->tamBloques);
			if(blok==0){
				leidoTotal=malloc(strlen(leido)+1);
				strcpy(leidoTotal,leido);
				free(leido);
			}else{
				leidoTotal=realloc(leidoTotal,strlen(leido));
				strcat(leidoTotal,leido);
				free(leido);
			}
			blok++;
		}
	offset-=metaLFS->tamBloques;
	}
	if(offset>0){
		if(bloques[blok]!=NULL){
			int nroBloq=atoi(bloques[blok]);
			char *pathB=rutaBloqueNro(nroBloq);
			char *leido=leerArchBinario(pathB,offset);
			if(blok==0){
				leidoTotal=malloc(strlen(leido)+1);
				strcpy(leidoTotal,leido);
				free(leido);
			}else{
				leidoTotal=realloc(leidoTotal,strlen(leido));
				strcat(leidoTotal,leido);
				free(leido);
			}
		}

	}
	return registrosLeidos=deChar_Registros(leidoTotal);//el leidoTotal se libera en la funcion esta
}

t_list *deChar_Registros(char *buffer){
	t_list *registros=list_create();
	while(strlen(buffer)>0){
		char **substring=string_n_split(buffer,4,";");
		Registry *nuevo=createRegistry(atoi(substring[1]),substring[2], atol(substring[0]));
		list_add(registros,nuevo);
		free(buffer);
		buffer=malloc(strlen (substring[3])+1);
		strcpy(buffer,substring[3]);
		free(substring[0]);    //LO HICE ASI PORQUE NO SE COMO MOD LA FUNCION LIB SUBSTRINGS
		free(substring[1]);
		free(substring[2]);
		free(substring[3]);
		free(substring);
	}
	free(buffer); //no se si esta de mas
	return registros;
}

char *leerArchBinario(char *path,int tamanio){
	FILE *arch;
	arch=fopen(path,"rb");
	char *datos=malloc(tamanio);
	fread(datos, sizeof(char), tamanio, arch);
	fclose(arch);
	return datos;
}

//****************************************************************************************
//FUNCIONES EXPERIMENTALES DE BITMAPS

void ocuparBloque(int Nrobloque){
	char*rutaBloque=rutaBloqueNro(Nrobloque);
//mutex
	FILE* file = fopen(rutaBloque,"wb");
	fflush(file);
	fclose(file);
	free(rutaBloque);
	bitarray_set_bit(bitmap, Nrobloque);
	msync(bitmap->bitarray,bitmap->size,MS_SYNC);
//signal
}
void desocuparBloque(int Nrobloque){
	bitarray_clean_bit(bitmap, Nrobloque);
	msync(bitmap->bitarray,bitmap->size,MS_SYNC);
}

int obtenerBloqueVacio(){
	int i = 0;
	while(i<metaLFS->cantBloques && bitarray_test_bit(bitmap, i)){
		i++;
	}
	if (i < metaLFS->cantBloques){
		ocuparBloque(i);
		return i;
	}else{
		return -1;
	}
}
bool hayXBloquesLibres(int cantidad){
	//BLOQUEO BINARIO
	int libres = 0;
	libres= cantBloquesLibres(cantidad);
	return libres>=cantidad;
}
int cantBloquesLibres(int cantidad){
	int cont=0;
	int libres=0;
	while(cont<metaLFS->cantBloques){
		if(!bitarray_test_bit(bitmap, cont)){
			libres++;
		}
		if(cantidad!=0){
			if(libres>=cantidad ){
				break;
			}
		}
		cont++;
	}
	return libres;
}

char *cadenaDeRegistros(t_list *lista){
	char *buffer;
	int vacio=0;

	void sumarRegistro(Registry *reg){
		char *linea;
		linea=concatRegistro(reg);
		if(vacio==0){
			buffer=malloc(strlen(linea)+1);
			strcpy(buffer,linea);
			vacio++;
		}else{
			buffer=realloc(buffer,strlen(linea)+strlen(buffer)+1);
			strcat(buffer,linea);
		}
		buffer=ponerSeparador(buffer);
		//free(linea);
	}

	list_iterate(lista,(void*)sumarRegistro);
	return buffer;
}

int tamanioArchivo(char* path){
    int fd;
    struct stat fileInfo = {0};
    fd=open(path, O_RDONLY);
    if (fd == -1)
    {
        perror("Error al abrir el archivo.");
        exit(EXIT_FAILURE);
    }
    if (fstat(fd, &fileInfo) == -1)
    {
        perror("Error al obtener el tamanio.");
        return -1;
    }
	return fileInfo.st_size;
	close(fd);
}

void cargarBitmap(){
	char *rutaBitmap=nivelMetadata(2);
	archivoBitmap = open(rutaBitmap, O_RDWR | O_CREAT, S_IRWXU);
	int cantBytes=metaLFS->cantBloques/8;
	if(metaLFS->cantBloques%8!=0){
		cantBytes++;
	}
	struct stat buf;
	lseek(archivoBitmap, cantBytes, SEEK_SET);
	write(archivoBitmap, "", 1);
	fstat(archivoBitmap,&buf);
	char* bitArrayMap =  (char *) mmap(NULL, buf.st_size, PROT_WRITE | PROT_READ , MAP_SHARED, archivoBitmap, 0);
	bitmap = bitarray_create_with_mode(bitArrayMap, buf.st_size, MSB_FIRST);
}

void mostrarBitmap(){
	for(int i=0;i<(bitmap->size);i++){
		if(i%64==0 && i!=0){
			printf("\n");
		}
		printf("%d",bitarray_test_bit(bitmap,i));
	}
	printf("\n");
	if(hayXBloquesLibres(10)){
		printf("SI\n");
	}else{
		printf("NO\n");
	}
	//size_t i =bitarray_get_max_bit(bitmap);
}

int archivoValido(char *path){				//Devuelve 0 esta vacio o si no existe, si no 1
	FILE *archB;
	archB= fopen(path, "rb");
	if(archB!=NULL){
		size_t t = 1000;
		char *buffer=NULL;
		getline(&buffer,&t,archB);
		if(strlen(buffer)>0){
			return 1;
		}
		free(buffer);
		return 0;
	}else{
		return 0;
	}
}

int  escribirArchBinario(char *path,long timestamp,int key,char *value){

	char *linea=concatParaArchivo(timestamp,key,value,0);
	int base=strlen(linea)+1;

	FILE* particion=fopen(path,"wb");
	if(particion== NULL){
		printf("No se pudo abrir el archivo para escribir");
		return 1;
	}

	fwrite(linea,1,base,particion);

	fclose(particion);
	free(linea);

	return 0;
}

int  agregarArchBinario(char *path,long timestamp,int key,char *value){

	char *linea=concatParaArchivo(timestamp,key,value,1);
	int base=strlen(linea)+1;

	FILE* particion=fopen(path,"ab+");
	if(particion== NULL){
		printf("No se pudo abrir el archivo para escribir");
		return 1;
	}

	fwrite(linea,1,base,particion);

	fclose(particion);
	free(linea);

	return 0;
}
//**********************************************FALTA MODIFICAR
t_list *leerTodoArchBinario(char *path){
	t_list *registrosLeidos=list_create();

	FILE* particion=fopen(path,"rb");
	if(particion== NULL){
		printf("Error al abrir el archivo para leer");
		return registrosLeidos;
	}

	size_t t = 1000;

	char *buffer=NULL;

	while(!feof(particion)){
		getline(&buffer,&t,particion);
		printf("%s",buffer);
		if(strlen(buffer)>0){
			list_add(registrosLeidos,desconcatParaArch(buffer));
		}
		buffer=NULL;
	}

	free(buffer);
	return registrosLeidos;
}

int eliminarArchivo(char *path){
	if(remove(path)==0){
		return 0;
	}
	printf("Error al intentar borrar archivo");
	return 1;
}

void escribirReg(char *name,t_list *registros,int cantParticiones){
	int size=list_size(registros);
	while(size>0){
		Registry *nuevo=list_get(registros, size-1);
		int part=nuevo->key % cantParticiones;
		char *path=concatExtencion(name,part,1);
		if(archivoValido(path)!=0){
			agregarArchBinario(path,nuevo->timestamp,nuevo->key,nuevo->value);
		}else{
			escribirArchBinario(path,nuevo->timestamp,nuevo->key,nuevo->value);
		}
		free(path);
		size--;
	}
}

int contarTemporales(char *tabla){
	int cant=0;
	int seguir=1;
	while(seguir==1){
		char *rutaArch=nivelParticion(tabla,cant,1);
		FILE *f=fopen(rutaArch,"rb");
		if(f!=NULL){
			fclose(f);
			cant++;
		}else{
			seguir=0;
		}
	}
	return cant;
}

//*************************************************************************************
//FUNCIONES ASOCIADAS AL REGISTRO

Registry *createRegistry(u_int16_t key, char *val, long time){

	Registry *data = malloc(sizeof(Registry));

	data->timestamp = time;
	data->key = key;
	data ->value=malloc(strlen(val)+1);
	strcpy(data->value,val);

	return data;
}

void agregarRegistro(Tabla *name,u_int16_t key, char *val, long time){

	Registry *nuevo=createRegistry(key,val,time);
	list_add(name->registros,nuevo);
}

void destroyRegistry(Registry *self) {

    free(self->value);
    free(self);

}

int calcularIndex(t_list *lista,int key){
	int index=0;
	bool encontrar(Registry *es){
		index++;
		return es->key == key;
	}
	list_iterate(lista, (void*) encontrar);
	return index;
}

Registry *getList(){

	Registry *data = malloc(sizeof(Registry));
	data= list_get(memtable,0);

	return data;

}
Registry *keyConMayorTime(t_list *registros){
	Registry *mayor=NULL;
		void comparar(Registry *comparar){
			if(mayor==NULL){
				mayor=comparar;
			}else{
				if(mayor->timestamp <= comparar->timestamp){
					mayor=comparar;
				}
			}
		}
        list_iterate(registros, (void*) comparar);
	return mayor;
}
t_list *regDep(t_list *aDepu){
	t_list *depu=list_create();
	int cant=list_size(aDepu);

	while(cant>0){
		Registry* nuevo=list_get(aDepu, cant-1);
		if(list_is_empty(depu)){
			list_add(depu,nuevo);
		}else{
			if(encontrarKeyDepu(depu,nuevo->key)!=NULL){
				Registry *viejo=encontrarKeyDepu(depu,nuevo->key);
				if(viejo->timestamp <= nuevo->timestamp){
					int index= calcularIndex(depu,viejo->key);
					list_replace_and_destroy_element(depu, index-1, nuevo, (void*)destroyRegistry);
				}else{
					list_remove_and_destroy_element(aDepu, cant-1, (void*)destroyRegistry);
				}
			}else{
				list_add(depu,nuevo);
			}

		}

		nuevo=NULL;
		cant--;
	}
	return depu;
}

/**************************************************************************************************/
//FUNCIONES ASOCIADAS A TABLAS

Tabla *find_tabla_by_name(char *name) {
	int _is_the_one(Tabla *p) {

		return string_equals_ignore_case(p->nombre, name);

	}

	return list_find(memtable, (void*) _is_the_one);
}

Tabla *crearTabla(char *nombre,u_int16_t key, char *val, long time){
	Tabla *nueva=malloc(sizeof(Tabla));
	nueva->nombre=malloc(strlen(nombre)+1);
	strcpy(nueva->nombre,nombre);
	nueva->registros=list_create();
	list_add(nueva->registros,createRegistry(key,val,time));
	return nueva;
}

void liberarTabla(Tabla *self){
	free(self->nombre);
	list_destroy_and_destroy_elements(self->registros,(void*)destroyRegistry);
	free(self);
}
int encontrarRegistroPorKey(t_list *registros,int key){
//devuelve 1 cuando no hay registros de esa key, devuelve 0 cuando si hay
	bool existe(Registry *p) {
		return p->key == key;
	}

	if(list_is_empty(list_find(registros, (void*) existe))){
		return 1;
	}else{
		return 0;
	}
}

Registry *encontrarKeyDepu(t_list *registros,int key){
//devuelve 1 cuando no hay registros de esa key, devuelve 0 cuando si hay
	bool existe(Registry *p) {
		return p->key == key;
	}

	return list_find(registros, (void*) existe);
}

t_list* filtrearPorKey(t_list *registros,int key){
	bool misma_key(Registry *reg) {
		return reg->key == key;
	}
	return list_filter(registros, (void*) misma_key);
}
