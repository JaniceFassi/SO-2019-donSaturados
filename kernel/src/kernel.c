/*
 ============================================================================
 Name        : kernel.c
 Author      : pepe
 Version     :
 Copyright   : Your copyright notice
 Description : Hello World in C, Ansi-style
 ============================================================================
 */

#include "kernel.h"

int main(void) {
	logger = init_logger();
	config = read_config();
	inicializarColas();
	inicializarListas();
	//inicializa memoria x archivo de configuracion
	struct memoria *m1=malloc(sizeof(struct memoria));
	m1->id=0;
	m1->puerto=config_get_int_value(config,"PUERTO_MEMORIA");
	m1->estado=1;// inicia disponible
	list_add(memorias,m1);
	list_add(criterioSC,m1);
	// aca deberia poder conocer el resto de las memorias
	// ademas deberia hacer un describe de las tablas actuales
	pruebas();
	apiKernel();
	mostrarResultados();
	//char *linea=readline(">");
	/*
	char *a= "table1";
	char *b= "3";
	char* linea=malloc(40);
	strcpy(linea,"INSERT TABLE1 3 estaba la pajara pinta");
	parsear(linea);*/
	//conexionMemoria();

	//run("/home/utnso/Descargas/1C2019-Scripts-lql-checkpoint-master/animales.lql");

	return EXIT_SUCCESS;
}

t_log* init_logger() {
	return log_create("kernel.log", "kernel", 1, LOG_LEVEL_INFO);
}

t_config* read_config() {
	return config_create("/home/utnso/tp-2019-1c-donSaturados/kernel/kernel.config");
}

void apiKernel(){
	while(1){
		char * linea;
		linea= readline(">");
		if(string_starts_with(linea,"RUN")){
			char ** split= string_n_split(linea,2," ");
			run(split[1]);
		}
		else{
			if(string_starts_with(linea,"EXIT")||strcmp(linea,"")==0){
				return;
			}
			else{
				struct script *nuevo= malloc(sizeof(struct script));
				nuevo->lineasLeidas=0;
				nuevo->estado=1;
				nuevo->modoOp=1;// Script de una sola linea
				nuevo->input= linea;
				queue_push(ready,nuevo);
				ejecutarScripts();
			}
		}
	}
}

int conexionMemoria(){
		u_int16_t sock;
		char * ip = "192.168.0.80";
		u_int16_t port= 36015;
		int enviados;

		if(linkClient(&sock,ip , port,1)!=0){
			log_info(logger,"no se pudo conectar...");
		}

		char* select =malloc(13);
		char* insert=malloc(21);
		//char* create=malloc(18);
		char * describe=malloc(11);
		char * drop=malloc(11);

		strcpy(select,"010tablita;0");
		strcpy(insert,"118tablita;0;hola;20");
		//strcpy(create,"215tablita;SC;3;5");
		strcpy(describe,"308tablita");
		strcpy(drop,"408tablita");
		enviados=sendData(sock,select,strlen(select)+1);				//select
		enviados=sendData(sock,insert,strlen(insert)+1);	//insert
		//enviados=sendData(sock,create,strlen(create)+1);		//create
		enviados=sendData(sock,describe,strlen(describe)+1);				//describe con parametro
		enviados=sendData(sock,drop,strlen(drop)+1);				//drop
		return 0;
}

void run(char *path){
	queue_push(new,path);
	log_info(logger,"Script con el path %s entro a cola new",path);

	struct script *nuevo= malloc(sizeof(struct script));
	nuevo->lineasLeidas=0;
	nuevo->estado=1;
	nuevo->modoOp=0;

	char * pathReady=queue_pop(new);
	nuevo->input=string_from_format("%s", pathReady);

	queue_push(ready,nuevo);
	log_info(logger,"Script con el path %s entro a cola ready",path);
	ejecutarScripts();
}

void ejecutarScripts(){
	int limiteProcesamiento=config_get_int_value(config, "MULTIPROCESAMIENTO");
	while(!queue_is_empty(ready)){
		if(queue_size(exec)<=limiteProcesamiento){
			struct script *execNuevo = queue_pop(ready);
			queue_push(exec,execNuevo);
			log_info(logger,"Script nro %i entro a cola EXEC",execNuevo->id);
			if(execNuevo->modoOp==1){// es para scripts de una sola linea introducidos desde consola
				parsear(execNuevo->input);
			}
			else{
				FILE *f;
				f=fopen("/home/utnso/Descargas/1C2019-Scripts-lql-checkpoint-master/animales.lql","r");
				int quantum = config_get_int_value(config, "QUANTUM");
				if(f==NULL){
						log_info(logger,"Error al abrir el archivo");
				}
				if(execNuevo->lineasLeidas>0){
					f=avanzarLineas(execNuevo->lineasLeidas,f);
				}
				size_t a=1024;
				char *linea=NULL;
				int lineasEjecutadas=0;
				getline(&linea,&a,f);
				do{
					parsear(linea);
					lineasEjecutadas++;
					execNuevo->lineasLeidas++;
				}while(getline(&linea,&a,f)!=EOF && lineasEjecutadas<quantum);
				if(!feof(f)){
					log_info(logger,"Script nro %i salio por fin de quantum",execNuevo->id);
					queue_pop(exec);
					queue_push(ready,execNuevo);
				}
				else{
					queue_pop(exec);
					queue_push(myExit,execNuevo);
					log_info(logger,"Script con el path %s entro a cola exit",execNuevo->input);
				}
				fclose(f);
			}
		}
	}
}
FILE* avanzarLineas(int num,FILE * fp){
	int conta=num-1;
	fseek(fp, 0, SEEK_SET);
	prueba:
	if (conta>0)
	{
		while (fgetc (fp) != '\n');
		conta--;
		goto prueba;
	}
	return fp;
}

void parsear(char * linea){
	char ** split;
	if(string_starts_with(linea,"SELECT")){
		split = string_n_split(linea,3," ");
		mySelect(split[1],split[2]);
	}
	if(string_starts_with(linea,"INSERT")){
		split = string_n_split(linea,4," ");
		insert(split[1],split[2],split[3]);
	}
	if(string_starts_with(linea,"DROP")){
		split = string_n_split(linea,2," ");
		drop(split[1]);
	}
	if(string_starts_with(linea,"CREATE")){
		split = string_n_split(linea,5," ");
		create(split[1],split[2],split[3],split[4]);
	}
	if(string_starts_with(linea,"DESCRIBE")){
		split = string_n_split(linea,2," ");
		describe(split[1]);
	}
	if(string_starts_with(linea,"JOURNAL")){
		journal();
	}
	if(string_starts_with(linea,"ADD")){
		split= string_n_split(linea,3," ");
		add(split[1],split[2]);
	}
	if(string_starts_with(linea,"METRICS")){
		//metrics();
	}
}

void mySelect(char * table, char *key){
	int op= 0;
	char *linea= malloc(strlen(table)+strlen(key)+3);
	linea=string_from_format("%s;%s",table,key);
	int len = strlen(linea);
	char* msj= malloc(4+strlen(linea));
	char* tamanioYop= malloc(4);
	if(len>10){
		tamanioYop=string_from_format("%i%i",op,len);
	}
	else{
		tamanioYop=string_from_format("%i0%i",op,len);
	}
	msj=string_from_format("%s%s",tamanioYop,linea);

	log_info(logger,"SELECT %s",msj);

	//aca fijandome la tabla deberia elegir la memoria adecuada

	// faltaria el send, probar mañana

	free(linea);
	free(msj);
	free(tamanioYop);
}

void insert(char* table ,char* key ,char* value){
	int op= 1;
	char *linea= malloc(strlen(table)+strlen(key)+3+strlen(value));
	linea=string_from_format("%s;%s;%s",table,key,value);
	int len = strlen(linea);
	char* msj= malloc(4+strlen(linea));
	char* tamanioYop= malloc(4);
	if(len>10){
		tamanioYop=string_from_format("%i%i",op,len);
	}
	else{
		tamanioYop=string_from_format("%i0%i",op,len);
	}
	msj=string_from_format("%s%s",tamanioYop,linea);

	log_info(logger,"INSERT %s",msj);

	//aca fijandome la tabla deberia elegir la memoria adecuada
	// faltaria el send, probar mañana

	free(linea);
	free(msj);
	free(tamanioYop);
}

void create(char* table , char* consistency , char* numPart , char* timeComp){

	log_info(logger,"HOLA CREATE");

}

void journal(){

}

void describe(char *table){
	log_info(logger,"HOLA DESCRIBE");
}

void drop(char*table){

}

void add(char* memory , char* consistency){
	u_int16_t idMemoria = atoi(memory);
	struct memoria *memoria= malloc(sizeof(struct memoria));
	memoria= buscarMemoria(idMemoria);
	if(memoria==NULL){
		log_info(logger,"La memoria no existe, no se pudo agregar al criterio");
		return;
	}
	if(strcmp(consistency,"SC")==0){
		if(!verificaMemoriaRepetida(idMemoria,criterioSC)){
			list_add(criterioSC,memoria);
		}
		else{
			log_info(logger,"No se pudo agregar la memoria %i al criterio SC porque esta repetida", idMemoria);
		}
	}
	if(strcmp(consistency,"SHC")==0){
		if(!verificaMemoriaRepetida(idMemoria,criterioSHC)){
			list_add(criterioSHC,memoria);
		}
		else{
			log_info(logger,"No se pudo agregar la memoria %i al criterio SHC porque esta repetida", idMemoria);
		}
	}
	if(strcmp(consistency,"EC")==0){
		if(!verificaMemoriaRepetida(idMemoria,criterioEC)){
			list_add(criterioEC,memoria);
		}
		else{
			log_info(logger,"No se pudo agregar la memoria %i al criterio EC porque esta repetida", idMemoria);
		}
	}
}

bool verificaMemoriaRepetida(u_int16_t id, t_list*criterio){
	bool idRepetido(struct memoria *mem){
		return mem->id==id;
	}
	return list_any_satisfy(criterio,(void*) idRepetido);
}


struct memoria * buscarMemoria(u_int16_t id){
	int funcionBusca(struct memoria *mem){
		return mem->id==id;
	}
	return list_find(memorias,(void*)funcionBusca);
}

void inicializarColas(){
	myExit=queue_create();
	ready=queue_create();
	new=queue_create();
	exec=queue_create();
}

void inicializarListas(){
	memorias=list_create();
	criterioSC=list_create();
	criterioSHC=list_create();
	criterioEC=list_create();
}

struct memoria *asignarMemoriaSegunCriterio(char* key, char *consistency){
	struct memoria *memAsignada= malloc(sizeof(struct memoria));
	if(strcmp(consistency,"SC")==0){
		memAsignada=verMemoriaLibre(criterioSC);
	}
	if(strcmp(consistency,"SHC")==0){
		memAsignada=verMemoriaLibre(criterioSHC);
	}
	if(strcmp(consistency,"EC")==0){
		memAsignada=verMemoriaLibre(criterioEC);
	}
	return memAsignada;
}

struct memoria *verMemoriaLibre(t_list *lista){
	int memFind(struct memoria *mem){
		return mem->estado==1;
	}
	return list_find(lista,(void*) memFind);
}

void pruebas(){
	struct memoria *m2= malloc(sizeof(struct memoria));
	m2->id=1;
	m2->estado=1;
	m2->puerto=8002;
	struct memoria *m3= malloc(sizeof(struct memoria));
	m3->id=2;
	m3->estado=1;
	m3->puerto=8003;
	struct memoria *m4= malloc(sizeof(struct memoria));
	m4->id=3;
	m4->estado=1;
	m4->puerto=8004;
	list_add(memorias,m2);
	list_add(memorias,m3);
	list_add(memorias,m4);
}

void mostrarResultados(){
	struct memoria *mem= malloc(sizeof(struct memoria));
	int i=0;
	int len = list_size(criterioSC);
	mem=list_get(criterioSC,i);
	log_info(logger,"memorias agregadas en SC: \n");
	while(i<len){
		log_info(logger,"%i \n",mem->id);
		i++;
		mem=list_get(criterioSC,i);
	}
	i=0;
	len = list_size(criterioEC);
	mem=list_get(criterioEC,i);
	log_info(logger,"memorias agregadas en EC: \n");
	while(i<len){
		log_info(logger,"%i \n",mem->id);
		i++;
		mem=list_get(criterioEC,i);
	}
	i=0;
	len = list_size(criterioSHC);
	mem=list_get(criterioSHC,i);
	log_info(logger,"memorias agregadas en SHC: \n");
	while(i<len){
		log_info(logger,"%i \n",mem->id);
		i++;
		mem=list_get(criterioSHC,i);
	}
}
