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

// Falta describe COMPLETO (hablar con memoria),--
// create(HECHO, falta probarlo)--
// deberia poner los estados como ENUM porque nadie los va a entender (ya fue, ni en pedo)

int main(void) {
	srand(time(NULL));
	logger = init_logger();
	config = read_config();
	inicializarColas();
	inicializarListas();
	quantum = config_get_int_value(config, "QUANTUM");
	//inicializa memoria x archivo de configuracion
	struct memoria *m1=malloc(sizeof(struct memoria));
	m1->id=0;
	m1->ip=config_get_string_value(config,"IP_MEMORIA");
	m1->puerto=config_get_int_value(config,"PUERTO_MEMORIA");
	m1->estado=1;// inicia disponible
	list_add(memorias,m1);
	list_add(criterioSC,m1);
/*	int limiteProcesamiento=config_get_int_value(config, "MULTIPROCESAMIENTO");
	pthread_t hilos[limiteProcesamiento];
	int i=0;
	while(i<limiteProcesamiento){
		pthread_create(&(hilos[i]), NULL, (void*)ejecutarScripts, NULL);
		i++;
	}*/
	apiKernel();
	/*
	// aca deberia poder conocer el resto de las memorias
	// ademas deberia hacer un describe de las tablas actuales
	pthread_t hiloDescribe;
	pthread_create(&hiloDescribe, NULL, (void*)describeGlobal, NULL);

	pruebas();
	//apiKernel();
	run("/home/utnso/Descargas/1C2019-Scripts-lql-checkpoint-master/animales.lql");
//	run("/home/utnso/Descargas/1C2019-Scripts-lql-checkpoint-master/comidas.lql");
//	run("/home/utnso/Descargas/1C2019-Scripts-lql-checkpoint-master/misc_1.lql");
	//run("/home/utnso/Descargas/1C2019-Scripts-lql-checkpoint-master/misc_2.lql");
	//run("/home/utnso/Descargas/1C2019-Scripts-lql-checkpoint-master/películas.lql");
*/
/*	i=0;
	while(i<limiteProcesamiento){
		pthread_join(hilos[i], NULL);
		i++;
	}*/
//	pthread_join(hiloDescribe, NULL);

	//ejecutarScripts();
//	mostrarResultados();
//	destruir();

	ejecutarScripts();

	return EXIT_SUCCESS;
}

t_log* init_logger() {
	return log_create("kernel.log", "kernel", 1, LOG_LEVEL_INFO);
}

t_config* read_config() {
	return config_create("/home/utnso/tp-2019-1c-donSaturados/kernel/kernel.config");
}

void apiKernel(){
	//while(1){
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
				nuevo->id=idScriptGlobal;
				//sem3 wait
				idScriptGlobal++;
				//sem3 signal
				nuevo->lineasLeidas=0;
				nuevo->estado=0;
				nuevo->modoOp=1;// Script de una sola linea
				nuevo->input= linea;
				queue_push(ready,nuevo);
				//signal de ejecutar scripts?

			}
		}
//	}
}

int conexionMemoria(int puerto){
		u_int16_t sock;
		char * ip = config_get_string_value(config,"IP_MEMORIA");
		u_int16_t port= puerto;
		//int enviados;
		if(linkClient(&sock,ip , port,1)!=0){
			return -1;
		}
		/*char* select =malloc(13);
		char* insert=malloc(21);
		char* create=malloc(18);
		char * describe=malloc(11);
		char * drop=malloc(11);
		strcpy(select,"010tablita;0");
		strcpy(insert,"118tablita;0;hola;20");
		strcpy(create,"215tablita;SC;3;5");
		strcpy(describe,"308tablita");
		strcpy(drop,"408tablita");
		enviados=sendData(sock,select,strlen(select)+1);				//select
		enviados=sendData(sock,insert,strlen(insert)+1);	//insert
		enviados=sendData(sock,create,strlen(create)+1);		//create
		enviados=sendData(sock,describe,strlen(describe)+1);				//describe con parametro
		enviados=sendData(sock,drop,strlen(drop)+1);				//drop
		*/
		return sock;
}

void run(char *path){
	queue_push(new,path);
	log_info(logger,"Script con el path %s entro a cola new",path);

	struct script *nuevo= malloc(sizeof(struct script));
	nuevo->id=idScriptGlobal;

	idScriptGlobal++;

	nuevo->lineasLeidas=0;
	nuevo->estado=0;
	nuevo->modoOp=0;

	char * pathReady=queue_pop(new);
	nuevo->input=string_from_format("%s", pathReady);

	queue_push(ready,nuevo);
	// signal de ejecutar scripts?
	log_info(logger,"Script con el path %s entro a cola ready",path);
}

void ejecutarScripts(){
	int resultado=0;
	while(terminaHilo==0){
		if(!queue_is_empty(ready)){
			// otro semaforo que indique que se puede pasar en vez del if?
			// sem1 wait
			struct script *execNuevo = queue_pop(ready);
			queue_push(exec,execNuevo);
			// sem1 signal
			log_info(logger,"Script nro %i entro a cola EXEC",execNuevo->id);
			if(execNuevo->modoOp==1){// es para scripts de una sola linea introducidos desde consola
				resultado=parsear(execNuevo->input);
				if(resultado!=0){
					//sem1 wait
					queue_pop(exec);
					execNuevo->estado=1;//fallo
					queue_push(myExit,execNuevo);
					//sem1 signal
				}
				else{
					//sem1 wait
					queue_pop(exec);
					queue_push(myExit,execNuevo);
					//sem1 signal
				}
			}
			else{
				FILE *f;
				//f=fopen("/home/utnso/Descargas/1C2019-Scripts-lql-checkpoint-master/animales.lql","r");
				f=fopen(execNuevo->input,"r");

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
					resultado=parsear(linea);
					if(resultado!=0){
						//sem1 wait
						queue_pop(exec);
						execNuevo->estado=1;//fallo
						queue_push(myExit,execNuevo);
						//sem1 signal
					}
					lineasEjecutadas++;
					execNuevo->lineasLeidas++;
				}while(getline(&linea,&a,f)!=EOF && lineasEjecutadas<quantum && resultado==0);
				free(linea);
				if(!feof(f)){
					if(lineasEjecutadas>=quantum && resultado==0){
						//sem1 wait
						queue_pop(exec);
						queue_push(ready,execNuevo);
						//sem1 signal
						log_info(logger,"Script nro %i salio por fin de quantum",execNuevo->id);
					}
					else{
						log_info(logger,"El script fallo");
					}
				}
				else{
					//sem1 wait
					queue_pop(exec);
					queue_push(myExit,execNuevo);
					//sem1 signal
					log_info(logger,"Script con el path %s entro a cola exit",execNuevo->input);
				}
				fclose(f);

			}
		}
		if(queue_size(myExit)==1){
				terminaHilo=1;
			}
	}
}
FILE* avanzarLineas(int num,FILE * fp){
	int conta=num;
	fseek(fp, 0, SEEK_SET);
	while (conta>0){
		if(fgetc (fp) == '\n'){
			conta--;
		}
	}
	return fp;
}

int parsear(char * linea){
	char ** split;
	int resultado=1;
	if(string_starts_with(linea,"SELECT")){
		split = string_n_split(linea,3," ");
		resultado=mySelect(split[1],split[2]);
	}
	if(string_starts_with(linea,"INSERT")){
		split = string_n_split(linea,4," ");
		resultado=insert(split[1],split[2],split[3]);
	}
	if(string_starts_with(linea,"DROP")){
		split = string_n_split(linea,2," ");
		resultado=drop(split[1]);
	}
	if(string_starts_with(linea,"CREATE")){
		split = string_n_split(linea,5," ");
		resultado=create(split[1],split[2],split[3],split[4]);
	}
	if(string_starts_with(linea,"DESCRIBE")){
		split = string_n_split(linea,2," ");
		resultado=describe(split[1]);
	}
	if(string_starts_with(linea,"JOURNAL")){
		resultado=journal();
	}
	if(string_starts_with(linea,"ADD")){
		split= string_n_split(linea,3," ");
		resultado=add(split[1],split[2]);
	}
	if(string_starts_with(linea,"METRICS")){
		//resultado=metrics();
	}
	int i=0;
	while(split[i]!=NULL){
		free(split[i]);
		i++;
	}
	free(split);
	return resultado;
}

int mySelect(char * table, char *key){
	int op= 0;
	char*linea=string_from_format("%s;%s",table,key);
	int len = strlen(linea);
	char* tamanioYop;//= malloc(4);
	if(len>=100){
		tamanioYop=string_from_format("%i%i",op,len);
	}
	else{
		if(len>=10){
			tamanioYop=string_from_format("%i0%i",op,len);
		}
		else{
			tamanioYop=string_from_format("%i00%i",op,len);
		}
	}
	char *msj=string_from_format("%s%s",tamanioYop,linea);

	log_info(logger,"SELECT %s",msj);
	log_info(logger,"mide %i",strlen(msj));

	//aca fijandome la tabla deberia elegir la memoria adecuada
	//struct metadataTabla * metadata = malloc(sizeof(struct metadataTabla));
//	metadata = buscarMetadataTabla(table);
	struct memoria* memAsignada = malloc(sizeof(struct memoria));
	memAsignada= asignarMemoriaSegunCriterio("SC");

	int sock = conexionMemoria(memAsignada->puerto);

	int enviados=sendData(sock,msj,strlen(msj)+1);
	log_info(logger,"%i",enviados);

	char * resultado=malloc(2);
	recvData(sock,resultado,1);

	log_info(logger,"resultado %i" , atoi(resultado));
	/*
	if(atoi(resultado)!=0){
		if(atoi(resultado)==2){
			sendData(sock,"6",2);//journal
			recvData(sock,&resultado,1);
			if(atoi(resultado)!=0){
				return -1;
			}
			sendData(sock,msj,strlen(msj)+1);
			recvData(sock,&resultado,1);
			if(atoi(resultado)!=0){
				return -1;
			}
		}
		else{
			return -1;
		}
	}

	free(memAsignada);
	free(linea);
	free(msj);
	free(tamanioYop);*/
	return 0;
}

int insert(char* table ,char* key ,char* value){
	int op= 1;
	char*linea=string_from_format("%s;%s;%s",table,key,value);
	int len = strlen(linea);
	char* tamanioYop;;//= malloc(4);
	if(len>10){
		tamanioYop=string_from_format("%i%i",op,len);
	}
	else{
		tamanioYop=string_from_format("%i0%i",op,len);
	}
	char*msj=string_from_format("%s%s",tamanioYop,linea);

	log_info(logger,"INSERT %s",msj);
/*
	struct metadataTabla * metadata = malloc(sizeof(struct metadataTabla));
	metadata = buscarMetadataTabla(table);
	struct memoria* memAsignada = malloc(sizeof(struct memoria));
	memAsignada= asignarMemoriaSegunCriterio(metadata->consistency);

	int sock = conexionMemoria(memAsignada->puerto);

	sendData(sock,msj,strlen(msj)+1);

	char * resultado=malloc(2);
	recvData(sock,&resultado,1);

	if(atoi(resultado)!=0){
		if(atoi(resultado)==2){
			sendData(sock,"6",2);//journal
			recvData(sock,&resultado,1);
			if(atoi(resultado)!=0){
				return -1;
			}
			sendData(sock,msj,strlen(msj)+1);
			recvData(sock,&resultado,1);
			if(atoi(resultado)!=0){
				return -1;
			}
		}
		else{
			return -1;
		}
	}

	free(memAsignada);*/
	free(linea);
	free(msj);
	free(tamanioYop);
	return 0;
}

int create(char* table , char* consistency , char* numPart , char* timeComp){
	int op= 2;
	char*linea=string_from_format("%s;%s;%s;%s",table,consistency,numPart,timeComp);
	int len = strlen(linea);
	char* tamanioYop;//= malloc(4);
	if(len>10){
		tamanioYop=string_from_format("%i%i",op,len);
	}
	else{
		tamanioYop=string_from_format("%i0%i",op,len);
	}
	char*msj=string_from_format("%s%s",tamanioYop,linea);

	log_info(logger,"CREATE %s",msj);

	//tengo que hacer la metadata de la tabla? o despues con el describe? quedaria inconsistente hasta que lo hagan
/*	struct memoria* memAsignada = malloc(sizeof(struct memoria));
	memAsignada= asignarMemoriaSegunCriterio(consistency);

	int sock = conexionMemoria(memAsignada->puerto);

	sendData(sock,msj,strlen(msj)+1);

	char * resultado=malloc(2);
	recvData(sock,&resultado,1);

	if(atoi(resultado)!=0){
		return -1;
	}

	free(memAsignada);*/
	free(linea);
	free(msj);
	free(tamanioYop);
	return 0;
}

int journal(){
	char msj= "5JOURNAL";
	//aca deberia mandar a cada una supongo
	return 0;
}

int describe(char *table){
	int tamanio=0;
	if(table!=NULL){
		tamanio= strlen(table);
	}
	int op=3;//verificar
	char *msj;
	if(tamanio==0){
		msj=string_from_format("%i%i",op,tamanio);
	}
	else{
		msj=string_from_format("%i%i%s",op,tamanio,table);
	}

	log_info(logger,"DESCRIBE %s",msj);
/*
	struct memoria* memAsignada = malloc(sizeof(struct memoria));
	memAsignada= verMemoriaLibre(memorias);

	int sock = conexionMemoria(memAsignada->puerto);

	sendData(sock,msj,strlen(msj)+1);



	char *resultado=malloc(2);
	recvData(sock,resultado,1);

	if(atoi(resultado)!=0){
		return -1;
	}
	char *tamanioRespuesta= malloc(4);
	recvData(sock,tamanioRespuesta,3);
	int tr= atoi(tamanioRespuesta);
	int i=0;

	while(i<tr){
		char *t=malloc(3);
		recvData(sock,t,2);
		char *buffer=malloc(atoi(t)+1);
		recvData(sock,buffer,atoi(t));
		//REVISAR
		struct metadataTabla *metadata;//= malloc(sizeof(struct metadataTabla));
		char ** split = string_split(buffer,";");
		metadata->table=split[0];
		metadata->consistency=split[1];
		metadata->numPart=atoi(split[2]);
		metadata->compTime=atoi(split[3]);
		// sem2 wait?
		list_add(listaMetadata,metadata);
		// sem2 signal?
		free(t);
		free(buffer);
		i++;
	}
*/
//	free(resultado);
//	free(tamanioRespuesta);
	free(msj);
//	free(memAsignada);
	return 0;
}

int drop(char*table){
	int op=4;
	int len=strlen(table);
	char*msj=string_from_format("%i%i%s",op,len,table);

	struct metadataTabla * metadata = malloc(sizeof(struct metadataTabla));
	metadata = buscarMetadataTabla(table);
	struct memoria* memAsignada = malloc(sizeof(struct memoria));
	memAsignada= asignarMemoriaSegunCriterio(metadata->consistency);

	int sock = conexionMemoria(memAsignada->puerto);

	sendData(sock,msj,strlen(msj)+1);

	char * resultado=malloc(2);
	recvData(sock,&resultado,1);

	// deberia borrar la metadata de la tabla? (iria un semaforo)

	if(atoi(resultado)!=0){
		return -1;
	}
	log_info(logger,"INSERT %s",msj);

	free(msj);
	free(resultado);
	free(metadata);
	free(memAsignada);
	return 0;
}

int add(char* memory , char* consistency){
	u_int16_t idMemoria = atoi(memory);
	struct memoria *memoria;//= malloc(sizeof(struct memoria));
	memoria= buscarMemoria(idMemoria);
	if(memoria==NULL){
		log_info(logger,"La memoria no existe, no se pudo agregar al criterio");
		return -1;
	}
	if(strcmp(consistency,"SC")==0){
		if(list_size(criterioSC)>0){
			log_info(logger,"No se pudo agregar la memoria %i al criterio SC", idMemoria);
			return -1;
		}
		else{
			// semN wait (pongo N xq no se si poner un semaforo para cada lista o con poner una sirve)
			list_add(criterioSC,memoria);
			// semN signal
			return 0;
		}
	}
	if(strcmp(consistency,"SHC")==0){
		if(!verificaMemoriaRepetida(idMemoria,criterioSHC)){
			list_add(criterioSHC,memoria);
			return 0;
		}
		else{
			log_info(logger,"No se pudo agregar la memoria %i al criterio SHC porque esta repetida", idMemoria);
			return -1;
		}
	}
	if(strcmp(consistency,"EC")==0){
		if(!verificaMemoriaRepetida(idMemoria,criterioEC)){
			list_add(criterioEC,memoria);
			return 0;
		}
		else{
			log_info(logger,"No se pudo agregar la memoria %i al criterio EC porque esta repetida", idMemoria);
			return -1;
		}
	}
	return -1;
}

void metrics(){

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
	listaMetadata=list_create();
}

struct memoria *asignarMemoriaSegunCriterio(char *consistency){
	struct memoria *memAsignada= malloc(sizeof(struct memoria));
	if(strcmp(consistency,"SC")==0){
		memAsignada=list_get(criterioSC,0);
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
	int size= list_size(lista);
	int i= rand()%(size+1);
	return list_get(lista,i);
}

struct metadataTabla * buscarMetadataTabla(char* table){
	bool findTabla(struct metadataTabla *m){
		return strcmp(m->table,table)==0;
	}
	return list_find(listaMetadata,(void*) findTabla);
}

void describeGlobal(){
	int tiempo = config_get_int_value(config, "15000");
	while(1){
		sleep(tiempo/(float)1000);
		log_info(logger,"Describe global automatico papaa");
	}
}


//---------------- PRUEBAS ----------------------------

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
	struct memoria *mem;
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

void destruir(){
	log_destroy(logger);
	config_destroy(config);

	list_destroy(criterioEC);
	list_destroy(criterioSC);
	list_destroy(criterioSHC);

	void destruirMemorias(struct memoria *m){
		free(m);
	}
	list_destroy_and_destroy_elements(memorias,(void*)destruirMemorias);

	void destruirMet(struct metadataTabla *mt){
		free(mt->consistency);
		free(mt->table);
		free(mt);
	}
	list_destroy_and_destroy_elements(listaMetadata,(void*)destruirMet);

	void destruirNew(char*n){
		free(n);
	}
	queue_clean_and_destroy_elements(new,(void*)destruirNew);

	void destruirScript(struct script *s){
		free(s->input);
		free(s);
	}
	queue_clean_and_destroy_elements(ready,(void*) destruirScript);
	queue_clean_and_destroy_elements(exec,(void*) destruirScript);
	queue_clean_and_destroy_elements(myExit,(void*) destruirScript);

	free(new->elements);
	free(new);
	free(ready->elements);
	free(ready);
	free(myExit->elements);
	free(myExit);
	free(exec->elements);
	free(exec);
}





