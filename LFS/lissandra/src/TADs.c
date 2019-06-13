
/*
 * FileSystem.c
 *
 *  Created on: 12 abr. 2019
 *      Author: utnso
 */
#include "TADs.h"


/************************************************************************************************/
//FUNCIONES DE CONCATENAR

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
/////////////////////*************************************************************************

Registry *desconcatParaArch(char *linea){
	char **subString=string_n_split(linea,3,";");
	int key=atoi(subString[1]);
	long timestamp=atol(subString[0]);
	Registry *nuevo=createRegistry(key,subString[2],timestamp);
	return nuevo;
}//**************************************************************************************


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

/***************************************NUEVOS*****************************************************/
char *ponerBarra(char *linea){
	char *barra=malloc(2);
	strcpy(barra,"/");
	linea=realloc(linea,strlen(linea)+strlen(barra)+1);
	strcat(linea,barra);
	free(barra);
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
		path=extension(path,0);
		free(metadata);
	}
	return path;
}

char *nivelParticion(char *tabla, int particion){		//montaje/TABLAS/TABLA/part.bin
	char *path=nivelUnaTabla(tabla,2);
	char *part=string_itoa(particion);
	path=realloc(path,strlen(path)+strlen(part)+1);
	strcat(path,part);
	path=extension(path,0);
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

//***************************************************MODIFICADO JANI
int crearParticiones(metaTabla *tabla){
	FILE* arch; //BINARIO
	int cant=tabla->partitions;
	char *path;
	while(cant>0){
		path=nivelParticion(tabla->nombre,cant-1);
		if((arch= fopen(path,"wb"))<0){
			printf("Error al crear la particion numero %i de la tabla %s.",tabla->partitions, tabla->nombre);
			return 1;
		}
		crearMetaArchivo(path);
		fclose(arch);
		//
		cant --;
	}
//	free(path);
	return 0;
}

crearMetaArchivo(char *path){
	metaArch *nuevo=malloc(sizeof(metaArch));
	nuevo->size=0;
	//nuevo->bloques[0]=obtenerBloqueVacio();
	t_config *metaArch=config_create(path);
	char* size = string_itoa(nuevo->size);
	config_set_value(metaArch,"SIZE",size);
	//config_set_value(metaArch,"BLOCKS",nuevo->bloques);
	config_save(metaArch);
	config_destroy(metaArch);
	borrarMetaArch(nuevo);

	free(path);
}

borrarMetaArch(metaArch *archivo){
	free(archivo);
}

void crearMetadataTabla(char* nombre, char* consistency , u_int16_t numPartition,long timeCompaction){
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
	borrarMetadataTabla(nuevo);
	free(path);
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
void crearMetaLFS(u_int16_t size,u_int16_t cantBloques,char *magicNumber){
	char *path=nivelMetadata(1);
	FILE *arch=fopen(path,"wb");
	fclose(arch);
	metaLFS =malloc(sizeof(metaFileSystem));
	metaLFS->cantBloques=cantBloques;
	metaLFS->tamBloques=size;
	metaLFS->magicNumber=malloc(strlen(magicNumber)+1);
	strcpy(metaLFS->magicNumber,magicNumber);
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
leerMetaLFS(){
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
//****************************************************************************************
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
//***********************************************************************************************
int eliminarArchivo(char *path){
	if(remove(path)==0){
		return 0;
	}
	printf("Error al intentar borrar archivo");
	return 1;
}
//***************************************MODIFICADO
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
//*************************************************************************************
/**************************************************************************************************/
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
