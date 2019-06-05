/*
 * FileSystem.c
 *
 *  Created on: 12 abr. 2019
 *      Author: utnso
 */

#include "FileSystem.h"

/************************************************************************************************/
//FUNCIONES DE CONCATENAR

char *pathFinal(char *nombre, int principal){
	//0 ES LA CARP PRINCIPAL, 1 ES LA CARP DE LAS TABLAS, 2 PATH DE ARCH, 3 METADATA
	int base=string_length(puntoMontaje)+string_length(nombre)+2;
	char *pathF=malloc(base);
	strcpy(pathF,puntoMontaje);

	if(principal==0){
		strcat(pathF,nombre);
		return pathF;
	}

	char tab[]="Tablas/";
	base+=string_length(tab)+1;
	pathF=realloc(pathF,base);
	strcat(pathF,tab);
	strcat(pathF,nombre);

	if(principal==1){
		return pathF;
	}

	char barra[]="/";
	base +=string_length(barra)+1;
	pathF=realloc(pathF,base);
	strcat(pathF,barra);

	if(principal==2){
		return pathF;
	}

	char metadata[]="METADATA";
	base += string_length(metadata)+1;
	pathF=realloc(pathF,base);

	if(principal==3){
		strcat(pathF,metadata);
		return pathF;
	}
	free(pathF);
	return NULL;
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
		printf("No existe la Carpeta\n");
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

int crearParticiones(char *nombre, int cantidad){
	FILE* arch; //BINARIO

	while(cantidad>0){
		char *extencion=malloc(5);
		strcpy(extencion,".bin");
		char *part=string_itoa(cantidad-1);
		char *path=pathFinal(nombre,2);
		int base=strlen(path)+1+strlen(part)+1;
		path=realloc(path,base);
		strcat(path,part);
		free(part);
		base+=strlen(extencion)+1;
		path=realloc(path,base);
		strcat(path,extencion);
		free(extencion);
		if((arch= fopen(path,"wb"))<0){
			printf("Error al crear la particion numero %i de la tabla\n",cantidad);
			return 1;
		}
		fclose(arch);
		free(path);
		cantidad --;

	}
	return 0;
}

void crearArchMetadata(char* path, char* consistency , u_int16_t numPartition,long timeCompaction){
	metaTabla *nuevo=malloc(sizeof(metaTabla));
	nuevo->compaction_time=timeCompaction;
	nuevo->consistency= consistency;
	printf("%s",nuevo->consistency);
	nuevo->partitions= numPartition;
	printf("%i",nuevo->partitions);
	FILE* metadata= fopen(path,"wb"); //BINARIO
	//FILE* metadata= fopen(path,"w");    //TXT
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
	free(nuevo);
}

metaTabla *leerArchMetadata(char *path){
	t_config *metaTab=config_create(path);
	metaTabla *nuevo=malloc(sizeof(metaTabla));
	nuevo->compaction_time=config_get_long_value(metaTab, "COMPACTION_TIME");
	char *aux=config_get_string_value(metaTab, "CONSISTENCY");
	nuevo->consistency=malloc(string_length(aux));
	strcpy(nuevo->consistency,aux);
	nuevo->partitions= config_get_int_value(metaTab, "PARTITIONS");
	config_destroy(metaTab);
	return nuevo;
}

int  escribirArchBinario(char *path,long timestamp,int key,char *value){

	char *linea=concatParaArchivo(timestamp,key,value,0);
	int base=strlen(linea+1);

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
	int base=strlen(linea+1);

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

int leerTodoArchBinario(char *path){

	FILE* particion=fopen(path,"rb");
	if(particion== NULL){
		printf("Error al abrir el archivo para leer");
		return 1;
	}

	size_t t = 1000;

	char *buffer=NULL;

	while(!feof(particion)){
		getline(&buffer,&t,particion);
		printf("%s",buffer);
		buffer=NULL;
	}

	free(buffer);
	return 0;
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
		char *nombre=string_itoa(part);
		char *exten= malloc(5);
		strcpy(exten,".tmp");
		char *path=pathFinal(name,2);
		int base=strlen(path)+1+strlen(exten)+1+strlen(nombre)+1;
		path=realloc(path,base);
		strcat(path,nombre);
		strcat(path,exten);
		agregarArchBinario(path,nuevo->timestamp,nuevo->key,nuevo->value);
		free(nombre);
		free(exten);
		free(path);
		size--;
	}
}

/**************************************************************************************************/
//FUNCIONES ASOCIADAS AL REGISTRO

Registry *createRegistry(u_int16_t key, char *val, long time){

	Registry *data = malloc(sizeof(Registry));

	data->timestamp = time;
	data->key = key;
	data ->value=strdup(val);

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

	int cant2=list_size(depu);
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

		cant2=list_size(depu);
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
	nueva->nombre=malloc(strlen(nombre+1));
	strcpy(nueva->nombre,nombre);
	nueva->registros=list_create();
	list_add(nueva->registros,createRegistry(key,val,time));
	return nueva;
}

void liberarTabla(Tabla *self){
	free(self->nombre);
	list_destroy_and_destroy_elements(self->registros,(void *)destroyRegistry);
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
