#include "kernel.h"
#define EVENT_SIZE  ( sizeof (struct inotify_event) + 100 )
#define BUF_LEN     ( 1024 * EVENT_SIZE )
// tengo que sacar lo de los logs duplicados
int main(void) {
	sem_init(&semColasContador,0,0);
	sem_init(&semColasMutex,0,1);
	sem_init(&semMemorias,0,1);
	sem_init(&semMetadata,0,1);
	sem_init(&semMetricas,0,1);
	sem_init(&semConfig,0,1);
	srand(time(NULL));
	logger = init_logger(0);

	config = read_config();
	inicializarColas();
	inicializarListas();
	struct metrica *metricaSHC=malloc(sizeof(struct metrica));
	struct metrica *metricaSC=malloc(sizeof(struct metrica));
	struct metrica *metricaEC=malloc(sizeof(struct metrica));
	metricaEC->criterio=string_duplicate("EC");
	metricaSC->criterio=string_duplicate("SC");
	metricaSHC->criterio=string_duplicate("SHC");
	list_add(metricas,metricaEC);
	list_add(metricas,metricaSHC);
	list_add(metricas,metricaSC);
	inicializarMetricas();
	quantum = config_get_int_value(config, "QUANTUM");
	retardoMetadata = config_get_int_value(config, "METADATA_REFRESH");
	retardo=config_get_int_value(config, "SLEEP_EJECUCION");

	printf("ingrese el Id de la memoria conocida\n");
	scanf("%i", &idMem);
	//inicializa memoria x archivo de configuracion
	struct memoria *m1=malloc(sizeof(struct memoria));
	m1->id=idMem;
	m1->ip=config_get_string_value(config,"IP_MEMORIA");
	m1->puerto=config_get_int_value(config,"PUERTO_MEMORIA");
	m1->cantI=0;
	m1->cantS=0;

	list_add(memorias,m1);

	int limiteProcesamiento=config_get_int_value(config, "MULTIPROCESAMIENTO");

	pthread_t hilos[limiteProcesamiento];
	int i=0;
	while(i<limiteProcesamiento){
		pthread_create(&(hilos[i]), NULL, (void*)ejecutarScripts, NULL);
		i++;
	}
	// aca deberia poder conocer el resto de las memorias
	pthread_t hiloMetricas;
	pthread_create(&hiloMetricas, NULL, metricasAutomaticas, NULL);
	pthread_t hiloDescribe;
	pthread_create(&hiloDescribe, NULL, describeGlobal, NULL);
	pthread_t hiloGossip;
	pthread_create(&hiloGossip, NULL, (void*)gossiping, NULL);
	pthread_t hiloInotify;
	pthread_create(&hiloInotify, NULL, (void*)inotifyKernel, NULL);

	apiKernel();

	i=0;
	if(queue_size(ready)!=0){
		while(i<limiteProcesamiento){
			pthread_join(hilos[i], NULL);
			i++;
		}
	}

	destruir();


	return EXIT_SUCCESS;
}

t_log* init_logger(int i) {
	return log_create("kernel.log", "kernel",1, LOG_LEVEL_INFO);
}

t_config* read_config(){
	int c;
	printf("Ingrese el número de la configuracion a utilizar:");
	printf("\n1- Prueba base \n2- Prueba kernel\n3- Prueba lfs \n4- Prueba memoria\n5- Prueba stress\n");
	while(1){
		scanf("%i",&c);
		switch(c){
		case 1:
			return config_create("/home/utnso/tp-2019-1c-donSaturados/configsPruebas/base/kernel");
			break;
		case 2:
			return config_create("/home/utnso/tp-2019-1c-donSaturados/configsPruebas/kernel/kernel");
			break;
		case 3:
			return config_create("/home/utnso/tp-2019-1c-donSaturados/configsPruebas/lfs/kernel");
			break;
		case 4:
			return config_create("/home/utnso/tp-2019-1c-donSaturados/configsPruebas/memoria/kernel");
			break;
		case 5:
			return config_create("/home/utnso/tp-2019-1c-donSaturados/configsPruebas/stress/kernel");
			break;
		default:
			printf("Numero invalido, por favor ingrese de nuevo");
			break;
		}
	}
	return NULL;
}

void apiKernel(){
	while(1){
		char * linea;
		linea= readline(">");
		if(string_starts_with(linea,"RUN")){
			char ** split= string_n_split(linea,2," ");
			run(split[1]);
			int i=0;
			if(split!=NULL){
				while(split[i]!=NULL){
					free(split[i]);
					i++;
				}
				free(split);
			}
		}
		else{
			if(string_starts_with(linea,"EXIT")){
				terminaHilo=1;
				free(linea);
				return;
			}
			else{
				struct script *nuevo= malloc(sizeof(struct script));
				nuevo->id=idScriptGlobal;
				idScriptGlobal++;
				nuevo->lineasLeidas=0;
				nuevo->estado=0;
				nuevo->modoOp=1;// Script de una sola linea
				nuevo->input= string_from_format("%s0",linea);
				sem_wait(&semColasMutex);
				queue_push(ready,nuevo);
				sem_post(&semColasMutex);
				sem_post(&semColasContador);
			}
		}
		free(linea);
	}
}

int conexionMemoria(int puerto, char*ip){
		u_int16_t sock;
		u_int16_t port= puerto;
		log_info(logger,"puerto a conectar: %i" , port);
		log_info(logger,"ip a conectar: %s" , ip);
		if(linkClient(&sock,ip , port,0)!=0){
			return -1;
		}
		return sock;
}

void run(char *path){
	sem_wait(&semColasMutex);
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
					log_info(logger,"El script fallo");
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
						sem_wait(&semColasMutex);
						queue_pop(exec);
						queue_push(myExit,execNuevo);
						sem_post(&semColasMutex);
						log_info(logger,"El script fallo");
				}
				else{
					if(execNuevo->lineasLeidas>0){
						f=avanzarLineas(execNuevo->lineasLeidas,f);
					}
					size_t a=1024;
					char *linea=NULL;
					int lineasEjecutadas=0;
					sem_wait(&semConfig);
					int quantumAux = quantum;
					sem_post(&semConfig);

					getline(&linea,&a,f);
					do{
						resultado=parsear(linea);
						if(resultado!=0){
							execNuevo->estado=1;
						}
						lineasEjecutadas++;
						execNuevo->lineasLeidas++;
						free(linea);
						linea=NULL;
					}while(getline(&linea,&a,f)!=EOF && lineasEjecutadas<quantumAux && resultado==0);
					free(linea);
					if(!feof(f)){
						if(lineasEjecutadas>=quantumAux && resultado==0){
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
							log_error(logger,"El script fallo");
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
}
FILE* avanzarLineas(int num,FILE * fp){
	int conta=num;
//	fseek(fp, 0, SEEK_SET);
//	while (conta>0){
//		if(fgetc (fp) == '\n'){
//			conta--;
//		}
//	}

	char *p=NULL;
	size_t a = 1024;
	while(conta>0){
		getline(&p,&a,fp);
		free(p);
		p=NULL;
		conta--;
	}
	return fp;
}
int parsear(char * aux){
	char ** split=NULL;
	int resultado=1;
	char *linea= string_substring(aux,0,strlen(aux)-1);
	log_info(logger,linea);
	if(string_starts_with(linea,"SELECT")){
		split = string_split(linea," ");
		if(verificarParametros(split,3)==0){
			if(esNumero(split[2])==0){
				resultado=mySelect(split[1],split[2]);
			}
			else{
				log_error(logger,"ERROR - La key debe ser numérica");
			}
		}
		else{
			log_info(logger,"ERROR - La cantidad de parametros es incorrecta");
		}
	}
	if(string_starts_with(linea,"INSERT")){
		split = string_n_split(linea,4," ");
		if(verificarParametros(split,4)==0){
			if(esNumero(split[2])==0){
				if(split[3]!=NULL){
					resultado=insert(split[1],split[2],split[3]);
				}
			}
			else{
				log_error(logger,"ERROR - La key debe ser numérica");
			}
		}
		else{
			log_info(logger,"ERROR - La cantidad de parametros es incorrecta");
		}
	}
	if(string_starts_with(linea,"DROP")){
		split = string_split(linea," ");
		if(verificarParametros(split,2)==0){
			resultado=drop(split[1]);
		}
		else{
			log_info(logger,"ERROR - La cantidad de parametros es incorrecta");
		}
	}
	if(string_starts_with(linea,"CREATE")){
		split = string_split(linea," ");
		if(verificarParametros(split,5)==0){
			if(esNumero(split[3])==0){
				if(esNumero(split[4])==0){
					if(strcmp(split[2],"SC")==0 || strcmp(split[2],"SHC")==0 ||strcmp(split[2],"EC")==0 ){
						resultado=create(split[1],split[2],split[3],split[4]);
					}else{
						log_error(logger,"ERROR - No existe el criterio");
					}
				}
				else{
					log_error(logger,"ERROR - El tiempo de compactación debe ser numérico");
				}
			}
			else{
				log_error(logger,"ERROR - La cantidad de particiones debe ser numérica");
			}
		}
		else{
			log_info(logger,"ERROR - La cantidad de parametros es incorrecta");
		}

	}
	if(string_starts_with(linea,"DESCRIBE")){
		split = string_split(linea," ");
		resultado=describe(split[1]);
	}
	if(string_starts_with(linea,"JOURNAL")){
		resultado=journal();
	}
	if(string_starts_with(linea,"ADD")){
		split= string_n_split(linea,5," ");
		if(verificarParametros(split,5)==0){
			if(esNumero(split[2])==0){
				resultado=add(split[2],split[4]);
			}
			else{
				log_error(logger,"ERROR - El id de la memoria debe ser numérico");
			}
		}
		else{
			log_info(logger,"ERROR - La cantidad de parametros es incorrecta");
		}

	}
	if(string_starts_with(linea,"METRICS")){
		resultado=metrics(1);
	}
	int i=0;
	if(split!=NULL){
		while(split[i]!=NULL){
			free(split[i]);
			i++;
		}
		free(split);
	}
	free(linea);

	return resultado;
}

int mySelect(char * table, char *key){
    struct timeval ti, tf;
    double tiempo;
    gettimeofday(&ti, NULL);   // Instante inicial
    sem_wait(&semConfig);
    int retardoEjecucion=retardo;
    sem_post(&semConfig);
	usleep(retardoEjecucion*1000);
	int op= 0;
	char*linea=string_from_format("%s;%s",table,key);
	int len = strlen(linea)+1;
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

	struct metadataTabla * metadata;// = malloc(sizeof(struct metadataTabla));

	sem_wait(&semMetadata);
	metadata = buscarMetadataTabla(table);
	if(metadata==NULL){
		log_info(logger,"ERROR - no existe la tabla especificada");
		sem_post(&semMetadata);
		return 1;
	}
	char *cons = string_duplicate(metadata->consistency);
	sem_post(&semMetadata);


	struct memoria* memAsignada ;//= malloc(sizeof(struct memoria));

	sem_wait(&semMemorias);
	memAsignada= asignarMemoriaSegunCriterio(cons, key);
	sem_post(&semMemorias);
	int c=0;
	int sock=-1;
	while(sock==-1 && c<5 && memAsignada!=NULL){
			sock = conexionMemoria(memAsignada->puerto,memAsignada->ip);
		if(sock==-1){
			sem_wait(&semMemorias);
			sacarMemoriaCaida(memAsignada);
			memAsignada= asignarMemoriaSegunCriterio(cons, key);
			sem_post(&semMemorias);
		}
		c++;
	}
	if(sock==-1){
		log_info(logger, "no se pudo conectar con las memorias, se seguira con la ejecucion del script");
		return 0;
	}

	int enviados=sendData(sock,msj,strlen(msj)+1);
	//log_info(logger,"%i",enviados);
	log_info(logger,"Envie SELECT %s %s",table,key);
	char * resultado=malloc(2);
	recvData(sock,resultado,1);
	resultado[1]='\0';
	log_info(logger,"%i",atoi(resultado));
	if(atoi(resultado)==0){
		char *tamanioRta=malloc(4);
		recvData(sock,tamanioRta,3);
		tamanioRta[3]='\0';
		char *rta=malloc(atoi(tamanioRta));
		recvData(sock,rta,atoi(tamanioRta));
		log_info(logger,"Resultado SELECT : %s", rta);
		free(tamanioRta);
		/*if(consola==1){
			log_info(logger,"Resultado SELECT : %s",rta);
		}*/
	}
	if(atoi(resultado)==3){
		log_info(logger,"El select no tiene valor en esa key");
		close(sock);
		return 0;
	}
//	log_info(logger,"resultado entero SELECT: %i" , atoi(resultado));

	if(atoi(resultado)!=0){
		if(atoi(resultado)==2){
			log_info(logger,"\n JOURNAL \n");
			sendData(sock,"5",2);//journal
			recvData(sock,resultado,1);
			if(atoi(resultado)!=0){
				close(sock);
				return -1;
			}
			close(sock);
			sock = conexionMemoria(memAsignada->puerto,memAsignada->ip);
			sendData(sock,msj,strlen(msj)+1);
			recvData(sock,resultado,1);
			resultado[1]='\0';
			log_info(logger,"%i",atoi(resultado));
			if(atoi(resultado)==0){
				char *tamRta=malloc(4);
				recvData(sock,tamRta,3);
				tamRta[3]='\0';
				char *rtaS=malloc(atoi(tamRta));
				recvData(sock,rtaS,atoi(tamRta));
				log_info(logger,"\n\nResultado SELECT despues de journal: %s \n\n", rtaS);
			}
			else{
				log_info(logger,"El select no tiene valor en esa key");
			}
		}
		else{
			return -1;
		}
	}

	free(linea);
	free(msj);
	free(tamanioYop);
	close(sock);
    gettimeofday(&tf, NULL);   // Instante final
    tiempo= (tf.tv_sec - ti.tv_sec)*1000 + (tf.tv_usec - ti.tv_usec)/1000.0;
	sem_wait(&semMemorias);
	memAsignada->cantS++;
	sem_post(&semMemorias);

	sem_wait(&semMetricas);
	agregarAMetricas(cons,"S",tiempo);
	sem_post(&semMetricas);
	return 0;
}

int insert(char* table ,char* key ,char* value){
    struct timeval ti, tf;
    double tiempo;
    gettimeofday(&ti, NULL);   // Instante inicial
    sem_wait(&semConfig);
    int retardoEjecucion=retardo;
    sem_post(&semConfig);
	usleep(retardoEjecucion*1000);
	int op= 1;
	char **split= string_split(value,"\"");
	char*linea=string_from_format("%s;%s;%s",table,key,split[0]);

	int i=0;
	if(split!=NULL){
		while(split[i]!=NULL){
			free(split[i]);
			i++;
		}
		free(split);
	}

	int len = strlen(linea)+1;
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
	char*msj=string_from_format("%s%s",tamanioYop,linea);

//	log_info(logger,"INSERT %s",msj);

	struct metadataTabla * metadata ;//= malloc(sizeof(struct metadataTabla));
	sem_wait(&semMetadata);
	metadata = buscarMetadataTabla(table);
	if(metadata==NULL){
		log_info(logger,"ERROR - no existe la tabla especificada");
		sem_post(&semMetadata);
		return 1;
	}

	char *cons = string_duplicate(metadata->consistency);
	sem_post(&semMetadata);


	struct memoria* memAsignada ;//= malloc(sizeof(struct memoria));
	sem_wait(&semMemorias);
	memAsignada= asignarMemoriaSegunCriterio(cons , key);
	sem_post(&semMemorias);
	int c=0;
	int sock=-1;
	while(sock==-1 && c<5&&memAsignada!=NULL){
		sock = conexionMemoria(memAsignada->puerto,memAsignada->ip);

		if(sock==-1){
			sem_wait(&semMemorias);
			sacarMemoriaCaida(memAsignada);
			memAsignada= asignarMemoriaSegunCriterio(cons , key);
			sem_post(&semMemorias);
		}

		c++;
	}
	if(sock==-1){
		free(cons);
		log_info(logger, "no se pudo conectar con las memorias, se seguira con la ejecucion del script");
		return 0;
	}

	sendData(sock,msj,strlen(msj)+1);
	log_info(logger,"envie el insert");
	char * resultado=malloc(2);
	recvData(sock,resultado,1);
	resultado[1]='\0';
	log_info(logger,"resultado INSERT: %i", atoi(resultado));
	if(atoi(resultado)!=0){
		if(atoi(resultado)==2){
			log_info(logger,"\n\n\n JOURNAL \n\n\n");
			sendData(sock,"5",2);//journal
			char *res= malloc(2);
			recvData(sock,res,1);
			res[1]='\0';
			log_info(logger,"resultado del journal: %i",atoi(res));
			if(atoi(res)!=0){
				close(sock);
				free(cons);
				return -1;
			}
			/*char *resI= malloc(2);
			sendData(sock,linea,strlen(linea)+1);
			recvData(sock,resI,1);
			resI[1]='\0';
			log_info(logger,"resultado del insert despues del journal: %i",atoi(resI));
			if(atoi(resI)!=0){
				close(sock);
				return -1;
			}*/
		}
		else{
			close(sock);
			free(cons);
			return -1;
		}
	}

//	free(memAsignada);

	free(linea);
	free(msj);
	free(tamanioYop);
	close(sock);
    gettimeofday(&tf, NULL);   // Instante final
    tiempo= (tf.tv_sec - ti.tv_sec)*1000 + (tf.tv_usec - ti.tv_usec)/1000.0;
//    log_info(logger,"despues de tiempo:%f", tiempo);
	sem_wait(&semMetricas);
	agregarAMetricas(cons,"I",tiempo);
	sem_post(&semMetricas);
	sem_wait(&semMemorias);
	memAsignada->cantI++;
	sem_post(&semMemorias);
	free(cons);
	return 0;
}

int create(char* table , char* consistency , char* numPart , char* timeComp){
    sem_wait(&semConfig);
    int retardoEjecucion=retardo;
    sem_post(&semConfig);
	usleep(retardoEjecucion*1000);
	int op= 2;
	char*linea=string_from_format("%s;%s;%s;%s",table,consistency,numPart,timeComp);
	int len = strlen(linea)+1;

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
	char*msj=string_from_format("%s%s",tamanioYop,linea);

	struct metadataTabla *m = malloc(sizeof(struct metadataTabla));
	m->compTime= atoi(timeComp);
	m->consistency= string_duplicate(consistency);
	m->numPart= atoi(numPart);
	m->table=string_duplicate(table);
	sem_wait(&semMetadata);
	list_add(listaMetadata,m);
	sem_post(&semMetadata);

	//tengo que hacer la metadata de la tabla? o despues con el describe? quedaria inconsistente hasta que lo hagan
	struct memoria* memAsignada;// = malloc(sizeof(struct memoria));
	sem_wait(&semMemorias);
	memAsignada= asignarMemoriaSegunCriterio(consistency,NULL);
	sem_post(&semMemorias);
	int c=0;
	int sock=-1;
	while(sock==-1 && c<5&& memAsignada!=NULL){
		sock = conexionMemoria(memAsignada->puerto,memAsignada->ip);
		if(sock==-1){
			sem_wait(&semMemorias);
			sacarMemoriaCaida(memAsignada);
			memAsignada= asignarMemoriaSegunCriterio(consistency,NULL);
			sem_post(&semMemorias);
		}
		c++;
	}
	if(sock==-1){
		log_info(logger, "no se pudo conectar con las memorias, se seguira con la ejecucion del script");
		return 0;
	}

	sendData(sock,msj,strlen(msj)+1);
	log_info(logger,"Mande el create");
	char * resultado=malloc(2);
	recvData(sock,resultado,2);
	resultado[1]='\0';
	log_info(logger,"resultado CREATE: %i", atoi(resultado));

	if(atoi(resultado)!=0){
		close(sock);
		return -1;
	}

	free(linea);
	free(msj);
	free(tamanioYop);
	close(sock);
	return 0;
}

int journal(){
    sem_wait(&semConfig);
    int retardoEjecucion=retardo;
    sem_post(&semConfig);
	usleep(retardoEjecucion*1000);
	int ret=0;

	void envioJournal(struct memoria *m){
		char *resultado= malloc(2);
		int sock=conexionMemoria(m->puerto,m->ip);
		if(sock==-1){
			sacarMemoriaCaida(m);
			log_info(logger,"La memoria %i no pudo hacer el journal, la misma no esta activa",m->id);
		}
		else{
			sendData(sock,"5",2);
			recvData(sock,resultado,1);
			resultado[1]='\0';
			if(atoi(resultado)!=0){
				ret=1;
				log_info(logger,"La memoria %i no pudo hacer el journal",m->id);
			}
			free(resultado);
			close(sock);
		}
	}
	sem_wait(&semMemorias);
	list_iterate(criterioEC,(void*)envioJournal);
	list_iterate(criterioSC,(void*)envioJournal);
	list_iterate(criterioSHC,(void*)envioJournal);
	sem_post(&semMemorias);
	return ret;
}

int describe(char *table){
    sem_wait(&semConfig);
    int retardoEjecucion=retardo;
    sem_post(&semConfig);
	usleep(retardoEjecucion*1000);
	int tamanio=0;
	if(table!=NULL){
		tamanio= strlen(table)+1;
	}
	int op=3;//verificar
	char *msj;
	if(tamanio==0){
		msj=string_from_format("%i00%i",op,tamanio);
	}
	else{
		// ver temas de comunicacion con memoria
		if(tamanio<10){
			msj=string_from_format("%i00%i%s",op,tamanio,table);
		}else{
			if(tamanio<100){
				msj=string_from_format("%i0%i%s",op,tamanio,table);
			}
			else msj=string_from_format("%i%i%s",op,tamanio,table);
		}
	}


	struct memoria* memAsignada ;//= malloc(sizeof(struct memoria));
	sem_wait(&semMemorias);
	memAsignada= verMemoriaLibre(memorias);
	log_info(logger, "Memoria seleccionada para el describe %i", memAsignada->id);
	sem_post(&semMemorias);
	int c=0;
	int sock=-1;
	while(sock==-1 && c<5 && memAsignada!=NULL){
		sock = conexionMemoria(memAsignada->puerto,memAsignada->ip);

		if(sock==-1){
			sem_wait(&semMemorias);
			sacarMemoriaCaida(memAsignada);
			memAsignada= verMemoriaLibre(memorias);
			sem_post(&semMemorias);
		}
		c++;
	}
	if(sock==-1){
		log_info(logger, "no se pudo conectar con las memorias, se seguira con la ejecucion del script");
		return 0;
	}

	sendData(sock,msj,strlen(msj)+1);
	log_info(logger,"mande el describe");


	char *resultado=malloc(2);
	recvData(sock,resultado,1);
	resultado[1]='\0';

	log_info(logger,"resultado DESCRIBE: %i", atoi(resultado));

	if(atoi(resultado)!=0){
		close(sock);
		return -1;
	}
	char *cantTablas= malloc(3);
	recvData(sock,cantTablas,2);
	cantTablas[2]='\0';
	int tr= atoi(cantTablas);
	log_info(logger,"Cantidad de tablas del describe: %s",cantTablas);
	int i=0;

	sem_wait(&semMetadata);
	if(tr>1){
		limpiarMetadata();

		while(i<tr){
			char *t=malloc(4);
			recvData(sock,t,3);
			t[3]='\0';
			int tam = atoi(t);
			char *buffer=malloc(tam+1);
			recvData(sock,buffer,tam);
			char *realBuffer= string_substring(buffer, 0, tam);
			log_info(logger,"buffer : %s",realBuffer);//ACA SIEMPRE LLEGA BASURA PERO LO GUARDA BIEN
			//REVISAR
			struct metadataTabla *metadata= malloc(sizeof(struct metadataTabla));
			char ** split = string_split(realBuffer,";");
			metadata->table= string_duplicate(split[0]);
			metadata->consistency=string_duplicate(split[1]);
			metadata->numPart=atoi(split[2]);
			metadata->compTime=atol(split[3]);

			// sem2 wait?
			list_add(listaMetadata,metadata);
			// sem2 signal?

			free(t);
			free(buffer);
			free(realBuffer);
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
		t[3]='\0';
		int tam = atoi(t);
		char *buffer=malloc(tam+1);
		recvData(sock,buffer,tam);
		struct metadataTabla *metadata= malloc(sizeof(struct metadataTabla));
		char *realBuffer= string_substring(buffer, 0, tam);
		char ** split = string_split(realBuffer,";");
		metadata->table= string_duplicate(split[0]);
		metadata->consistency=string_duplicate(split[1]);
		metadata->numPart=atoi(split[2]);
		metadata->compTime=atol(split[3]);
		actualizarMetadataTabla(metadata);

		free(split[0]);
		free(split[1]);
		free(split[2]);
		free(split[3]);
		free(split);
		free(t);
//		log_info(logger,"termino free ");
		free(buffer);
	}
//	void itera(struct metadataTabla*m){
//		log_info(logger,"consistency %s",m->consistency);
//		log_info(logger,"table %s",m->table);
//		log_info(logger," compTime %d",m->compTime);
//		log_info(logger,"numPart %i",m->numPart);
//	}
//	list_iterate(listaMetadata,(void*)itera);
	sem_post(&semMetadata);

	free(resultado);
//	free(tamanioRespuesta);
	free(msj);
	free(cantTablas);
//	log_info(logger,"entra a free mem ");
	close(sock);
//	log_info(logger,"sale fun ");
	return 0;
}

int drop(char*table){
    sem_wait(&semConfig);
    int retardoEjecucion=retardo;
    sem_post(&semConfig);
	usleep(retardoEjecucion*1000);
	int op=4;
	int len=strlen(table)+1;

	char*msj;

	if(len<10){
		msj=string_from_format("%i00%i%s",op,len,table);
	}else{
		if(len<100){
			msj=string_from_format("%i0%i%s",op,len,table);
		}else msj=string_from_format("%i%i%s",op,len,table);
	}

	struct metadataTabla * metadata;// = malloc(sizeof(struct metadataTabla));
	sem_wait(&semMetadata);
	metadata = buscarMetadataTabla(table);
	if(metadata==NULL){
		log_info(logger,"ERROR - no existe la tabla especificada");
		sem_post(&semMetadata);
		return 1;
	}
	char *cons = string_duplicate(metadata->consistency);
	sem_post(&semMetadata);




	struct memoria* memAsignada; //= malloc(sizeof(struct memoria));
	sem_wait(&semMemorias);
		memAsignada= asignarMemoriaSegunCriterio(cons,NULL);
	sem_post(&semMemorias);
	int c=0;
	int sock=-1;
	while(sock==-1 && c<5 && memAsignada!=NULL){
			sock = conexionMemoria(memAsignada->puerto,memAsignada->ip);
		if(sock==-1){
			sem_wait(&semMemorias);
				sacarMemoriaCaida(memAsignada);
				memAsignada= asignarMemoriaSegunCriterio(cons,NULL);
			sem_post(&semMemorias);
		}
		c++;
	}
	if(sock==-1){
		log_info(logger, "no se pudo conectar con las memorias, se seguira con la ejecucion del script");
		return 0;
	}

	sendData(sock,msj,strlen(msj)+1);
	log_info(logger,"Mande el drop");
	char * resultado=malloc(2);
	recvData(sock,resultado,1);
	resultado[1]='\0';

	// deberia borrar la metadata de la tabla? (iria un semaforo)

	if(atoi(resultado)!=0){
		close(sock);
		return -1;
	}
	void destruirMet(struct metadataTabla *mt){
		free(mt->consistency);
		free(mt->table);
		free(mt);// <=== si rompe por doble free es x esto
	}

	bool buscar(struct metadataTabla *mt){
		return strcmp(mt->table,table)!=0;
	}
	sem_wait(&semMetadata);
	list_remove_and_destroy_by_condition(listaMetadata,(void*) buscar,(void*) destruirMet);
	sem_post(&semMetadata);


	free(msj);
	free(resultado);
	close(sock);
	return 0;
}

int add(char* memory , char* consistency){
    sem_wait(&semConfig);
    int retardoEjecucion=retardo;
    sem_post(&semConfig);
	usleep(retardoEjecucion*1000);
	u_int16_t idMemoria = atoi(memory);
	struct memoria *memoria;//= malloc(sizeof(struct memoria));
	sem_wait(&semMemorias);
	memoria= buscarMemoria(idMemoria);
	sem_post(&semMemorias);

	if(memoria==NULL){
		log_info(logger,"La memoria no existe, no se pudo agregar al criterio");
		return 0;
	}
	if(memoria->estado==0){
		if(strcmp(consistency,"SC")==0){
			sem_wait(&semMemorias);
			int size=list_size(criterioSC);
			sem_post(&semMemorias);
			if(size>0){
				log_info(logger,"No se pudo agregar la memoria %i al criterio SC", idMemoria);
				return -1;
			}
			else{
				sem_wait(&semMemorias);
					list_add(criterioSC,memoria);
					log_info(logger,"Se agrego la memoria %i al criterio SC", idMemoria);
				sem_post(&semMemorias);			// semN wait (pongo N xq no se si poner un semaforo para cada lista o con poner una sirve)
				// semN signal
				return 0;
			}
		}
		if(strcmp(consistency,"SHC")==0){
			sem_wait(&semMemorias);
			bool repe=verificaMemoriaRepetida(idMemoria,criterioSHC);
			sem_post(&semMemorias);
			if(!repe){
				int ret=0;
				sem_wait(&semMemorias);
					list_add(criterioSHC,memoria);
				void envioJournal(struct memoria *m){
					char *resultado= malloc(2);
					int sock=conexionMemoria(m->puerto,m->ip);
					if(sock==-1){
						sacarMemoriaCaida(m);
						log_info(logger,"La memoria %i no pudo hacer el journal, la misma esta caida",m->id);
					}
					else{
						sendData(sock,"5",2);
						recvData(sock,resultado,1);
						resultado[1]='\0';
						if(atoi(resultado)!=0){
							ret=1;
							log_info(logger,"La memoria %i no pudo hacer el journal",m->id);

						}
						close(sock);
					}
					free(resultado);
				}
				list_iterate(criterioSHC,(void*)envioJournal);
				log_info(logger,"Se agrego la memoria %i al criterio SHC", idMemoria);
				sem_post(&semMemorias);
				return 0;
			}
			else{
				log_info(logger,"No se pudo agregar la memoria %i al criterio SHC porque esta repetida", idMemoria);
				return -1;
			}
		}
		if(strcmp(consistency,"EC")==0){
			sem_wait(&semMemorias);
			bool repe= verificaMemoriaRepetida(idMemoria,criterioEC);
			sem_post(&semMemorias);
			if(!repe){
				sem_wait(&semMemorias);
				list_add(criterioEC,memoria);
				sem_post(&semMemorias);
				log_info(logger,"Se agrego la memoria %i al criterio EC", idMemoria);
				return 0;
			}
			else{
				log_info(logger,"No se pudo agregar la memoria %i al criterio EC porque esta repetida", idMemoria);
				return -1;
			}
		}
	}
	else{
		log_info(logger, "La memoria no esta activa, no pudo ser asignada al criterio");
		return -1;
	}
	return -1;
}

int metrics(int modo){
    sem_wait(&semConfig);
    int retardoEjecucion=retardo;
    sem_post(&semConfig);
	usleep(retardoEjecucion*1000);
	sem_wait(&semMetricas);
	int totalOp=0;
	void iterador(struct metrica *metrica){
		float promedioS=(float)((metrica->tiempoS)/metrica->cantS);
		float promedioI=(float)((metrica->tiempoI)/metrica->cantI);
		log_info(logger,"\n------------METRICAS--------------\n");
		log_info(logger,"Criterio %s:",metrica->criterio);
		log_info(logger,"Read Latency: %f",promedioS);
		log_info(logger,"Write Latency: %f",promedioI);
		log_info(logger,"Reads: %i",metrica->cantS);
		log_info(logger,"Writes: %i",metrica->cantI);

		totalOp+= metrica->cantI+metrica->cantS;
	}

	list_iterate(metricas,(void*)iterador);
	sem_post(&semMetricas);


	log_info(logger,"MEMORY LOAD:");
	void itera(struct memoria *m){
		log_info(logger,"En la memoria %i se hicieron %i INSERT/SELECT de los %i totales",m->id,m->cantI+m->cantS,totalOp);
		if(modo==0){
			m->cantI=0;
			m->cantS=0;
		}
	}
	sem_wait(&semMemorias);
	list_iterate(memorias,(void*)itera);
	sem_post(&semMemorias);
	return 0;
}

void *metricasAutomaticas(){
	while(terminaHilo==0){
		sleep(30);
		metrics(0);
		//semaforos
		inicializarMetricas();
	}
	return NULL;
}
void agregarAMetricas(char *cons , char *op , double tiempo){
	void busca(struct metrica *m ){
		if(strcmp(m->criterio,cons)==0){
			if(strcmp(op,"S")==0){
				m->cantS++;
				m->tiempoS+=tiempo;
			}else{
				m->cantI++;
				m->tiempoI+=tiempo;
			}
		}
	}
	list_iterate(metricas,(void*)busca);
}
void inicializarMetricas(){
	sem_wait(&semMetricas);
	void itera(struct metrica *m){
		m->cantS=0;
		m->tiempoS=0;
		m->cantI=0;
		m->tiempoI=0;
	}
	list_iterate(metricas,(void*)itera);
	sem_post(&semMetricas);
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
	log_info(logger,"limpiarMeta");
	void destruirMet(struct metadataTabla *mt){
		free(mt->consistency);
		free(mt->table);
		free(mt);
	}
//	sem_wait(&semMetadata);
	list_destroy_and_destroy_elements(listaMetadata,(void*)destruirMet);
	listaMetadata=list_create();
//	sem_post(&semMetadata);
}

void actualizarMetadataTabla(struct metadataTabla *m){
	// agregar semaforo

	void destruirMet(struct metadataTabla *mt){
		free(mt->consistency);
		free(mt->table);
		free(mt);
	}
	bool buscar(struct metadataTabla *mt){
		return strcmp(mt->table,m->table)==0;
	}
	bool buscarANY(struct metadataTabla *mt){
			return strcmp(mt->table,m->table)==0;
		}
	//sem_wait(&semMetadata);
	if(list_any_satisfy(listaMetadata,(void*)buscarANY)){
		list_remove_and_destroy_by_condition(listaMetadata,(void*) buscar,(void*) destruirMet);
	/*	struct metadataTabla aux = list_find(listaMetadata,(void*)buscar);

		strcpy(aux->compTime, m->compTime);
		aux->compTime= m->compTime;
		aux->numPart=m->numPart;
		aux->table=*/
	}
	list_add(listaMetadata,m);
	//sem_post(&semMetadata);
	log_info(logger,"consistency %s",m->consistency);
	log_info(logger,"table %s",m->table);
	log_info(logger,"compTime %d",m->compTime);
	log_info(logger,"numPart %i",m->numPart);
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
	metricas=list_create();
}

struct memoria *asignarMemoriaSegunCriterio(char *consistency, char *key){
	struct memoria *memAsignada;//= malloc(sizeof(struct memoria));
	if(strcmp(consistency,"SC")==0){
		if(!list_is_empty(criterioSC)){
			memAsignada=list_get(criterioSC,0);
		}
		else memAsignada=NULL;
	}
	if(strcmp(consistency,"SHC")==0){
		if(key==NULL){
			memAsignada=verMemoriaLibreSHC(0);
		}
		else{
			memAsignada=verMemoriaLibreSHC(atoi(key));
		}
	}
	if(strcmp(consistency,"EC")==0){
		memAsignada=verMemoriaLibre(criterioEC);
	}
	return memAsignada;
}

struct memoria *verMemoriaLibreSHC(int key){
	if(key==0){
		key=1;
	}
	int size= list_size(criterioSHC);
	if(size!=0){
		int idMemoria = (key+113)%size;

		struct memoria *m = list_get(criterioSHC,idMemoria);

		return m;
	}
	else{
		return NULL;
	}
}

struct memoria *verMemoriaLibre(t_list *lista){
	if(!list_is_empty(lista)){
		int size= list_size(lista);
		int i= rand()%(size);			//Se le saco el +1
		return list_get(lista,i);
	}
	else return NULL;
}

void sacarMemoriaCaida(struct memoria *m){
	log_info(logger,"La memoria %i esta caida, se procedera a eliminarla de los criterios asociados",m->id);
	bool sacar(struct memoria * mem){
		return mem->id==m->id;
	}
	if(list_any_satisfy(criterioSC,(void*)sacar)){
		list_remove_by_condition(criterioSC,(void*)sacar);
	}
	if(list_any_satisfy(criterioEC,(void*)sacar)){
		list_remove_by_condition(criterioEC,(void*)sacar);
	}
	if(list_any_satisfy(criterioSHC,(void*)sacar)){
		list_remove_by_condition(criterioSHC,(void*)sacar);
			void envioJournal(struct memoria *m){
				char *resultado= malloc(2);
				int sock=conexionMemoria(m->puerto,m->ip);
				if(sock!=-1){
					sendData(sock,"5",2);
					recvData(sock,resultado,1);
					resultado[1]='\0';
					if(atoi(resultado)!=0){
						log_info(logger,"La memoria %i no pudo hacer el journal",m->id);

					}
					close(sock);
				}
			}
			list_iterate(criterioSHC,(void*)envioJournal);
	}
	m->estado=1;//seteo que no esta activa (en el gossip puedo volverla a poner activa)
}

struct metadataTabla * buscarMetadataTabla(char* table){
	bool findTabla(struct metadataTabla *m){
		return strcmp(m->table,table)==0;
	}
	return list_find(listaMetadata,(void*) findTabla);
}

int esNumero(char *key){
	int r=0,i=0;
	if(key!=NULL){
		while(i<strlen(key)){
		if(isdigit(key[i])==0){
				r=1;
			}
			i++;
		}
		return r;
	}
	else return 1;
}

int verificarParametros(char **split,int cantParametros){
	int contador=0;
	while(contador<cantParametros){
		if(split[contador]==NULL){
			return 1;
		}
		else contador ++;
	}
	return 0;
}


void *describeGlobal(){
	while(terminaHilo==0){
	    sem_wait(&semConfig);
	    int retardoMet=retardoMetadata;
	    sem_post(&semConfig);
		usleep(retardoMet*1000);
		describe(NULL);
		log_info(logger,"Describe global automático");
	}
	return NULL;
}




void *gossiping(){
	while(1){
		log_info(logger, "GOSSIP");
	//	sleep(1);// NO TENGO DE DONDE SACAR ESTE DATO(lo pase abajo de todo)
		int sock=-1;
		struct memoria *m ;
		int termina=0;
		while(sock==-1 && termina <5){
			sem_wait(&semMemorias);
				m=verMemoriaLibre(memorias);
			sem_post(&semMemorias);
			sock= conexionMemoria(m->puerto,m->ip);
			if(sock==-1){
				sem_wait(&semMemorias);
				sacarMemoriaCaida(m);
				sem_post(&semMemorias);
			}
			termina++;
		}
		if(sock!=-1){
			sendData(sock,"6",2);
			char* salioBien = malloc(2);
			recvData(sock, salioBien, 1);
			salioBien[1]='\0';
			if(atoi(salioBien)==7){
				char *tamanio= malloc(4);
				recvData(sock,tamanio,3);
				tamanio[3]='\0';
				char *buffer=malloc(atoi(tamanio)+1);
				recvData(sock,buffer,atoi(tamanio));
				free(tamanio);
				log_info(logger,"\n\n");
				log_info(logger,buffer);
				log_info(logger,"\n\n");
				close(sock);
				//toda la bola del gossip
				sem_wait(&semMemorias);
				void itera(struct memoria *m){
					m->estado=1;
				}
				list_iterate(memorias,(void*) itera);
				char ** split= string_n_split(buffer,4,";");
				free(buffer);
				char * bufferAux=NULL;
				int termino=0;
				while(termino==0){
					if(verificaMemoriaRepetida(atoi(split[0]),memorias)){
						struct memoria *aux=buscarMemoria(atoi(split[0]));
						aux->estado=0;
						}
					else{
						struct memoria *aux= malloc(sizeof(struct memoria));
						aux->cantI=0;
						aux->cantS=0;
						aux->estado=0;
						aux->id=atoi(split[0]);
						aux->ip=string_duplicate(split[1]);
						aux->puerto=atoi(split[2]);
						list_add(memorias,aux);
						}
					if(split[3]!=NULL){
						bufferAux= string_duplicate(split[3]);
						}

					free(split[0]);
					free(split[1]);
					free(split[2]);
					free(split[3]);
					free(split);

					if(bufferAux==NULL){
						termino=1;
					}
					else{
						split=string_n_split(bufferAux,4,";");
						}
					free(bufferAux);
					bufferAux=NULL;
				}
				log_info(logger,"Fin del gossiping, el estado de las memorias conocidas es:");

				void sacaMemoriasCaidas(struct memoria *m){
					log_info(logger,"mem %i puerto %i ip %s" , m->id , m->puerto , m->ip);
					if(m->estado==1){
						sacarMemoriaCaida(m);
						log_info(logger,"Memoria %i: CAIDA" , m->id);
					}
					else{
						log_info(logger,"Memoria %i: ACTIVA", m->id);
					}
				}
				list_iterate(memorias,(void*)sacaMemoriasCaidas);
				sem_post(&semMemorias);
				}
			}
		else{
			log_info(logger,"No se pudo hacer el gossiping, no se pudo conectar con las memorias");
		}
				sleep(10);
		}
	return NULL;

}

void *inotifyKernel(){
	while(terminaHilo==0){
			char buffer[BUF_LEN];
			int file_descriptor = inotify_init();
			if (file_descriptor < 0) {
				perror("inotify_init");
			}
			int watch_descriptor = inotify_add_watch(file_descriptor, "/home/utnso/tp-2019-1c-donSaturados/configsPruebas/kernel/kernel", IN_MODIFY );
			int length = read(file_descriptor, buffer, BUF_LEN);
			if (length < 0) {
				perror("read");
			}
			t_config *configInotify = config_create("/home/utnso/tp-2019-1c-donSaturados/configsPruebas/kernel/kernel");
			//semaforo wait
			sem_wait(&semConfig);
			quantum=config_get_int_value(configInotify, "QUANTUM");
			retardo=config_get_int_value(configInotify, "SLEEP_EJECUCION");
			retardoMetadata=config_get_int_value(configInotify, "METADATA_REFRESH");

			log_info(logger,"Se efectuaron cambios en la configuracion:");
			log_info(logger,"Retardo Ejecucion: %i", retardo);
			log_info(logger,"Refresh metadata: %i", retardoMetadata);
			log_info(logger,"quantum: %i", quantum);
			sem_post(&semConfig);
			config_destroy(configInotify);
		}
		return NULL;
}

//---------------- PRUEBAS ----------------------------

void pruebas(){
	run("/home/utnso/Descargas/1C2019-Scripts-lql-entrega-master/scripts/compactacion_larga.lql");
	run("/home/utnso/Descargas/1C2019-Scripts-lql-checkpoint-master/comidas.lql");
	run("/home/utnso/Descargas/1C2019-Scripts-lql-entrega-master/scripts/cities_countries.lql");
//	run("/home/utnso/Descargas/1C2019-Scripts-lql-entrega-master/scripts/animales.lql");
//	run("/home/utnso/Descargas/1C2019-Scripts-lql-entrega-master/scripts/games_computer.lql");
//	run("/home/utnso/Descargas/1C2019-Scripts-lql-entrega-master/scripts/internet_browser.lql");
//	run("/home/utnso/Descargas/1C2019-Scripts-lql-checkpoint-master/animales_falla.lql");
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
	log_info(logger,"Se procedera a finalizar el sistema");
	log_info(logger,"Estado de los scripts ejecutados");


	log_destroy(logger);

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
	void destruirScriptExit(struct script *s){
		free(s->input);
		free(s);
	}
	queue_clean_and_destroy_elements(ready,(void*) destruirScript);
	queue_clean_and_destroy_elements(exec,(void*) destruirScript);
	queue_clean_and_destroy_elements(myExit,(void*) destruirScriptExit);


	void borrarM(struct metrica *m){
		free(m->criterio);
		free(m);
	}
	list_destroy_and_destroy_elements(metricas,(void*) borrarM);

	free(new->elements);
	free(new);
	free(ready->elements);
	free(ready);
	free(myExit->elements);
	free(myExit);
	free(exec->elements);
	free(exec);
}
