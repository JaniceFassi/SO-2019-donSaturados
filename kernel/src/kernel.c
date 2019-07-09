#include "kernel.h"

int main(void) {
	sem_init(&semColasContador,0,0);
	sem_init(&semColasMutex,0,1);
	srand(time(NULL));
	logger = init_logger();
	config = read_config();
	inicializarColas();
	inicializarListas();
	metrica.cantI=0;
	metrica.cantS=0;
	metrica.tiempoI=0;
	metrica.tiempoS=0;
	quantum = config_get_int_value(config, "QUANTUM");
	//inicializa memoria x archivo de configuracion
	struct memoria *m1=malloc(sizeof(struct memoria));
	m1->id=0;
	m1->ip=config_get_string_value(config,"IP_MEMORIA");
	m1->puerto=config_get_int_value(config,"PUERTO_MEMORIA");
	m1->estado=1;// inicia disponible
	list_add(memorias,m1);
	list_add(criterioSC,m1);
	pruebas();
	run("/home/utnso/Descargas/1C2019-Scripts-lql-checkpoint-master/animales.lql");
	run("/home/utnso/Descargas/1C2019-Scripts-lql-checkpoint-master/comidas.lql");
	int limiteProcesamiento=config_get_int_value(config, "MULTIPROCESAMIENTO");
	pthread_t hilos[limiteProcesamiento];
	int i=0;
	while(i<limiteProcesamiento){
		pthread_create(&(hilos[i]), NULL, (void*)ejecutarScripts, NULL);
		i++;
	}
	// aca deberia poder conocer el resto de las memorias
	pthread_t hiloMetricas;
	pthread_create(&hiloMetricas, NULL, (void*)metricasAutomaticas, NULL);
	pthread_t hiloDescribe;
	pthread_create(&hiloDescribe, NULL, (void*)describeGlobal, NULL);


//	run("/home/utnso/Descargas/1C2019-Scripts-lql-checkpoint-master/misc_1.lql");
	//run("/home/utnso/Descargas/1C2019-Scripts-lql-checkpoint-master/misc_2.lql");
	//run("/home/utnso/Descargas/1C2019-Scripts-lql-checkpoint-master/películas.lql");
	apiKernel();
	i=0;
	/*while(i<limiteProcesamiento){
		pthread_join(hilos[i], NULL);
		i++;
	}*/

//	mostrarResultados();
	destruir();

//	ejecutarScripts();

	return EXIT_SUCCESS;
}

t_log* init_logger() {
	return log_create("kernel.log", "kernel", 0, LOG_LEVEL_DEBUG);
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
				terminaHilo=1;
				return;
			}
			else{
				struct script *nuevo= malloc(sizeof(struct script));
				nuevo->id=idScriptGlobal;
				idScriptGlobal++;
				nuevo->lineasLeidas=0;
				nuevo->estado=0;
				nuevo->modoOp=1;// Script de una sola linea
				nuevo->input= linea;
				sem_wait(&semColasMutex);
				queue_push(ready,nuevo);
				sem_post(&semColasMutex);
				sem_post(&semColasContador);
			}
		}
	}
}

int conexionMemoria(int puerto, char*ip){
		u_int16_t sock;
		u_int16_t port= puerto;
		if(linkClient(&sock,ip , port,1)!=0){
			return -1;
		}
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
	sem_wait(&semColasMutex);
	queue_push(ready,nuevo);
	sem_post(&semColasMutex);
	sem_post(&semColasContador);
	log_info(logger,"Script con el path %s entro a cola ready",path);
}

void ejecutarScripts(){
	int resultado=0;
	while(terminaHilo==0){
			sem_wait(&semColasContador);
			sem_wait(&semColasMutex);
			struct script *execNuevo = queue_pop(ready);
			queue_push(exec,execNuevo);
			sem_post(&semColasMutex);

			log_info(logger,"Script nro %i entro a cola EXEC",execNuevo->id);
			if(execNuevo->modoOp==1){// es para scripts de una sola linea introducidos desde consola
				resultado=parsear(execNuevo->input);
				if(resultado!=0){
					execNuevo->estado=1;//fallo
					sem_wait(&semColasMutex);
					queue_pop(exec);
					queue_push(myExit,execNuevo);
					sem_post(&semColasMutex);
				}
				else{
					sem_wait(&semColasMutex);
					queue_pop(exec);
					queue_push(myExit,execNuevo);
					sem_post(&semColasMutex);
				}
			}
			else{
				FILE *f;
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
						execNuevo->estado=1;
					}
					lineasEjecutadas++;
					execNuevo->lineasLeidas++;
				}while(getline(&linea,&a,f)!=EOF && lineasEjecutadas<quantum && resultado==0);
				free(linea);
				if(!feof(f)){
					if(lineasEjecutadas>=quantum && resultado==0){
						sem_wait(&semColasMutex);
						queue_pop(exec);
						queue_push(ready,execNuevo);
						sem_post(&semColasMutex);
						sem_post(&semColasContador);
						log_info(logger,"Script nro %i salio por fin de quantum",execNuevo->id);
					}
					else{
						sem_wait(&semColasMutex);
						queue_pop(exec);
						queue_push(myExit,execNuevo);
						sem_post(&semColasMutex);
						log_info(logger,"El script fallo");
					}
				}
				else{
					sem_wait(&semColasMutex);
					queue_pop(exec);
					queue_push(myExit,execNuevo);
					sem_post(&semColasMutex);
					log_info(logger,"Script con el path %s entro a cola exit",execNuevo->input);
				}
				fclose(f);
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
		resultado=metrics(1);
	}
	int i=0;
	while(split[i]!=NULL){
		free(split[i]);
		i++;
	}
	free(split);
	int retardo= config_get_int_value(config,"SLEEP_EJECUCION");
	sleep(retardo/(float)1000);
	return resultado;
}

int mySelect(char * table, char *key){
	clock_t ini = clock();
	// va semaforo
	metrica.cantS++;

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
	struct metadataTabla * metadata = malloc(sizeof(struct metadataTabla));
	metadata = buscarMetadataTabla(table);
	if(metadata==NULL){
		log_info(logger,"ERROR - no existe la tabla especificada");
		return -1;
	}
	struct memoria* memAsignada = malloc(sizeof(struct memoria));
/*
	int c=0;
	int sock=-1;
	while(sock==-1 && c<5){
		memAsignada= asignarMemoriaSegunCriterio(metadata->consistency);
		if(memAsignada!=NULL){
			sock = conexionMemoria(memAsignada->puerto,memAsignada->ip);
		}
		c++;
	}
	if(sock==-1){
		return -1;
	}

	int enviados=sendData(sock,msj,strlen(msj)+1);
	log_info(logger,"%i",enviados);

	char * resultado=malloc(2);
	recvData(sock,resultado,1);

	log_info(logger,"resultado %i" , atoi(resultado));

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
	}*/
	free(memAsignada);
	free(linea);
	free(msj);
	free(tamanioYop);
	clock_t fin = clock();
	double tiempo = (double)(fin-ini);
	//semaforo
	metrica.tiempoS += tiempo;
	return 0;
}

int insert(char* table ,char* key ,char* value){
	clock_t ini = clock();
	// va semaforo
	metrica.cantI++;

	int op= 1;
	char **split= string_split(value,"\"");
	char*linea=string_from_format("%s;%s;%s",table,key,split[0]);
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

	struct metadataTabla * metadata = malloc(sizeof(struct metadataTabla));
	metadata = buscarMetadataTabla(table);

	if(metadata==NULL){
		log_info(logger,"ERROR - no existe la tabla especificada");
		return -1;
	}

	struct memoria* memAsignada = malloc(sizeof(struct memoria));
	/*int c=0;
	int sock=-1;
	while(sock==-1 && c<5){
		memAsignada= asignarMemoriaSegunCriterio(metadata->consistency);
		if(memAsignada!=NULL){
			sock = conexionMemoria(memAsignada->puerto,memAsignada->ip);
		}
		c++;
	}
	if(sock==-1){
		return -1;
	}

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
	}*/

	free(memAsignada);
	free(linea);
	free(msj);
	free(tamanioYop);

	clock_t fin = clock();
	double tiempo= (double) (fin-ini);
	//semaforo
	metrica.tiempoI+= tiempo;
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

	//prueba, luego borrar
	struct metadataTabla *m = malloc(sizeof(struct metadataTabla));
	m->compTime= atoi(timeComp);
	m->consistency= string_duplicate(consistency);
	m->numPart= atoi(numPart);
	m->table=string_duplicate(table);
	list_add(listaMetadata,m);

	//tengo que hacer la metadata de la tabla? o despues con el describe? quedaria inconsistente hasta que lo hagan
	struct memoria* memAsignada = malloc(sizeof(struct memoria));
/*
	int c=0;
	int sock=-1;
	while(sock==-1 && c<5){
		memAsignada= asignarMemoriaSegunCriterio(consistency);
		if(memAsignada!=NULL){
			sock = conexionMemoria(memAsignada->puerto,memAsignada->ip);
		}
		c++;
	}
	if(sock==-1){
		return -1;
	}

	sendData(sock,msj,strlen(msj)+1);

	char * resultado=malloc(2);
	recvData(sock,&resultado,1);

	if(atoi(resultado)!=0){
		return -1;
	}*/

	free(memAsignada);
	free(linea);
	free(msj);
	free(tamanioYop);
	return 0;
}

int journal(){
	int ret=0;
	void envioJournal(struct memoria *m){
		char *resultado= malloc(2);
		int sock=conexionMemoria(m->puerto,m->ip);
		sendData(sock,"5",2);
		recvData(sock,resultado,1);
		if(atoi(resultado)!=0){
			ret=1;
			log_info(logger,"La memoria %i no pudo hacer el journal",m->id);
		}
	}
	list_iterate(criterioEC,(void*)envioJournal);
	list_iterate(criterioSC,(void*)envioJournal);
	list_iterate(criterioSHC,(void*)envioJournal);
	return ret;
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
		// ver temas de comunicacion con memoria
		msj=string_from_format("%i%i%s",op,tamanio,table);
	}

	log_info(logger,"DESCRIBE %s",msj);

	struct memoria* memAsignada = malloc(sizeof(struct memoria));
	//ver si tengo que usar una memoria asignada al criterio de la tabla
	/*int c=0;
	int sock=-1;
	while(sock==-1 && c<5){
		memAsignada= verMemoriaLibre(memorias);
		if(memAsignada!=NULL){
			sock = conexionMemoria(memAsignada->puerto,memAsignada->ip);
		}
		c++;
	}
	if(sock==-1){
		return -1;
	}

	sendData(sock,msj,strlen(msj)+1);



	char *resultado=malloc(2);
	recvData(sock,resultado,1);

	if(atoi(resultado)!=0){
		return -1;
	}
	char *tamanioRespuesta= malloc(5);
	recvData(sock,tamanioRespuesta,4);
	char *cantTablas= malloc(3);
	recvData(sock,cantTablas,2);
	int tr= atoi(cantTablas);
	int i=0;

//	me falta un recv de la cantidad de tablas, ademas deberia borrar las tablas que tengo
//  si tengo un describe de una sola tabla tengo que actualizarla
	if(tr>1){
		limpiarMetadata();
		while(i<tr){
			char *t=malloc(4);
			recvData(sock,t,3);
			char *buffer=malloc(atoi(t)+1);
			recvData(sock,buffer,atoi(t));
			//REVISAR
			struct metadataTabla *metadata= malloc(sizeof(struct metadataTabla));
			char ** split = string_split(buffer,";");
			metadata->table= string_duplicate(split[0]);
			metadata->consistency=string_duplicate(split[1]);
			metadata->numPart=atoi(split[2]);
			metadata->compTime=atol(split[3]);

			// sem2 wait?
			list_add(listaMetadata,metadata);
			// sem2 signal?

			free(t);
			free(buffer);
			free(split[0]);
			free(split[1]);
			free(split[2]);
			free(split[3]);
			free(split);
			i++;
		}
	}
	else{
		char *t=malloc(4);
		recvData(sock,t,3);
		char *buffer=malloc(atoi(t)+1);
		recvData(sock,buffer,atoi(t));
		struct metadataTabla *metadata= malloc(sizeof(struct metadataTabla));
		char ** split = string_split(buffer,";");
		metadata->table= string_duplicate(split[0]);
		metadata->consistency=string_duplicate(split[1]);
		metadata->numPart=atoi(split[2]);
		metadata->compTime=atol(split[3]);
		actualizarMetadataTabla(metadata);

		free(t);
		free(buffer);
		free(split[0]);
		free(split[1]);
		free(split[2]);
		free(split[3]);
		free(split);
	}
	free(resultado);
	free(tamanioRespuesta);*/
	free(msj);
	free(memAsignada);
	return 0;
}

int drop(char*table){
	int op=4;
	int len=strlen(table);
	char*msj=string_from_format("%i%i%s",op,len,table);

	struct metadataTabla * metadata = malloc(sizeof(struct metadataTabla));
	metadata = buscarMetadataTabla(table);

	if(metadata==NULL){
		log_info(logger,"ERROR - no existe la tabla especificada");
		return -1;
	}

	struct memoria* memAsignada = malloc(sizeof(struct memoria));
	int c=0;
	int sock=-1;
	while(sock==-1 && c<5){
		memAsignada= asignarMemoriaSegunCriterio(metadata->consistency);
		if(memAsignada!=NULL){
			sock = conexionMemoria(memAsignada->puerto,memAsignada->ip);
		}
		c++;
	}
	if(sock==-1){
		return -1;
	}

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

int metrics(int modo){
	float promedioS=(float)(metrica.tiempoS/(float)metrica.cantS);
	float promedioI=(float)(metrica.tiempoI/(float)metrica.cantI);
	if(modo==1){
		log_info(logger,"Read Latency: %f",promedioS);
		log_info(logger,"Write Latency: %f",promedioI);
		log_info(logger,"Reads: %i",metrica.cantS);
		log_info(logger,"Writes: %i",metrica.cantI);
	}
	else{
		log_info(logger,"Read Latency: %f",promedioS);
		log_info(logger,"Write Latency: %f",promedioI);
		log_info(logger,"Reads: %i",metrica.cantS);
		log_info(logger,"Writes: %i",metrica.cantI);
	}
	return 0;
}

void metricasAutomaticas(){
	while(terminaHilo==0){
		sleep(3);
		metrics(0);
		//semaforos
		metrica.cantI=0;
		metrica.cantS=0;
		metrica.tiempoI=0;
		metrica.tiempoS=0;
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
// borro toda la metadata para no andar actualizando
void limpiarMetadata(){
	// agregar semaforo
	void destruirMet(struct metadataTabla *mt){
		free(mt->consistency);
		free(mt->table);
		free(mt);
	}
	list_destroy_and_destroy_elements(listaMetadata,(void*)destruirMet);
	listaMetadata=list_create();
}

void actualizarMetadataTabla(struct metadataTabla *m){
	// agregar semaforo
	void destruirMet(struct metadataTabla *mt){
		free(mt->consistency);
		free(mt->table);
		free(mt);
	}
	bool buscar(struct metadataTabla *mt){
		return strcmp(mt->table,m->table)!=0;
	}
	list_remove_and_destroy_by_condition(listaMetadata,(void*) buscar,(void*) destruirMet);
	list_add(listaMetadata,m);
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
	int tiempo = config_get_int_value(config, "METADATA_REFRESH");
	while(terminaHilo==0){
		sleep(tiempo/(float)1000);
		describe(NULL);
		log_info(logger,"Describe global automático");
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
