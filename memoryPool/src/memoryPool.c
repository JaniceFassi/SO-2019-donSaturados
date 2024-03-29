/*
 ============================================================================
 Name        : memoryPool.c
 Author      : mpelozzi
 Version     :
 Copyright   : Your copyright notice
 Description : Hello World in C, Ansi-style
 ============================================================================
 */

#include "memoryPool.h"
#define EVENT_SIZE  ( sizeof (struct inotify_event) + 100 )
#define BUF_LEN     ( 1024 * EVENT_SIZE )


int main(void) {
	deploy();
	int exito = inicializar();

	if(exito==1){
		log_error(logger, "Abortando ejecución");
		return 1;

	}
	pthread_t inotify;
	pthread_create(&inotify, NULL, correrInotify, NULL);
	pthread_t gossipTemporal;
	pthread_create(&gossipTemporal, NULL, gossipProgramado, NULL);
	pthread_t hiloConsola;
	pthread_create(&hiloConsola, NULL, consola, NULL);
	pthread_t gestorConexiones;
	pthread_create(&gestorConexiones, NULL, gestionarConexiones, NULL);
	pthread_t journalTemporal;
	pthread_create(&journalTemporal, NULL, journalProgramado, NULL);

	pthread_join(hiloConsola, (void*)&fin);
	if(fin == 1){
		pthread_cancel(gestorConexiones);
		pthread_cancel(inotify);
		pthread_cancel(journalTemporal);
		pthread_cancel(gossipTemporal);
		log_info(logger, "Se apagará la memoria de forma correcta");

	}

	finalizar();
	return EXIT_SUCCESS;
}

//---------------------------------------------------------//
//------------------AUXILIARES DE ARRANQUE----------------//
//-------------------------------------------------------//

void deploy(){
	pathConfig = malloc(strlen("/home/utnso/tp-2019-1c-donSaturados/configsPruebas/memoria/memn.config")+1);
	printf("Ingrese el numero correspondiente a la prueba que se está por realizar\n");
	int prueba;
	int id;
	printf("1 - Base \n2 - Kernel\n3 - Lissandra\n4 - Memoria\n5 - Stress\n");
	scanf("%d", &prueba);
	printf("Ingrese el ID de la memoria que desea levantar: ");
	scanf("%d", &id);
	printf("Prueba %d id %d \n", prueba, id);
	switch(prueba){
	case 1:
		pathConfig = string_from_format("/home/utnso/tp-2019-1c-donSaturados/configsPruebas/base/mem%d.config", id);
		break;
	case 2:
		pathConfig = string_from_format("/home/utnso/tp-2019-1c-donSaturados/configsPruebas/kernel/mem%d.config", id);
		break;
	case 3:
		pathConfig = string_from_format("/home/utnso/tp-2019-1c-donSaturados/configsPruebas/lfs/mem%d.config", id);
		break;
	case 4:
		pathConfig = string_from_format("/home/utnso/tp-2019-1c-donSaturados/configsPruebas/memoria/mem%d.config", id);
		break;
	case 5:
		pathConfig = string_from_format("/home/utnso/tp-2019-1c-donSaturados/configsPruebas/stress/mem%d.config", id);
		break;
	}
}

 int inicializar(){
	logger = init_logger();


	t_config* configuracion = read_config();
	config = malloc(sizeof(estructuraConfig));
	int tamanioMemoria = config_get_int_value(configuracion, "TAM_MEM");
	init_configuracion(configuracion);



	pthread_mutex_init(&lockTablaSeg, NULL);
	pthread_mutex_init(&lockTablaMarcos, NULL);
	pthread_mutex_init(&lockLRU, NULL);
	pthread_mutex_init(&lockConfig, NULL);
	pthread_mutex_init(&lockJournal, NULL);
	sem_init(&lockTablaMem, 0, 1);
	sem_init(&semJournal, 0, config->multiprocesamiento);

	memoria = calloc(1,tamanioMemoria);

	if(memoria == NULL){
		log_error(logger, "Falló el inicio de la memoria principal, abortando ejecución");
		return 1;
	}
	log_info(logger, "Se inicializo la memoria con tamanio %d", tamanioMemoria);


	maxValue = handshakeConLissandra(config->ipFS, config->puertoFS);

	//maxValue = 60;

	if(maxValue == 1){
		log_error(logger, "No se pudo recibir el handshake con LFS, abortando ejecución\n");
		return 1;
	}

	log_info(logger, "Tamanio máximo recibido de FS: %d", maxValue);

	offsetMarco = sizeof(long) + sizeof(u_int16_t) + maxValue;
	tablaMarcos = list_create();
	tablaSegmentos = list_create();
	marcosReemplazables = list_create();

	cantMarcos = tamanioMemoria/offsetMarco;
	for(int i=0; i<cantMarcos; i++){
		marco* unMarco = malloc(sizeof(marco));
		unMarco->nroMarco = i;
		unMarco->estaLibre = 0;
		pthread_mutex_init(&unMarco->lockMarco, NULL);
		unMarco->ultimoUso = time(NULL);
		list_add(tablaMarcos, unMarco);
	}

	config_destroy(configuracion);
	return 0;
}
 void init_configuracion(t_config* configuracion){
	 config->retardoJournal = config_get_int_value(configuracion,"RETARDO_JOURNAL")*1000;
	 config->retardoGossiping= config_get_int_value(configuracion, "RETARDO_GOSSIPING")*1000;
	 config->retardoMem = config_get_int_value(configuracion, "RETARDO_MEM")*1000;
	 config->retardoFS = config_get_int_value(configuracion, "RETARDO_FS")*1000;
	 config->puerto = config_get_int_value(configuracion, "PUERTO");
	 char* ip = config_get_string_value(configuracion, "IP");
	 config->ip = string_duplicate(ip);
	 char* ipFS = config_get_string_value(configuracion,"IP_FS");
	 config->ipFS = string_duplicate(ipFS);
	 config->puertoFS= config_get_int_value(configuracion,"PUERTO_FS");
	 config->ipSeeds= config_get_array_value(configuracion,"IP_SEEDS");
	 config->puertoSeeds = config_get_array_value(configuracion,"PUERTO_SEEDS");
	 config->id=config_get_int_value(configuracion,"MEMORY_NUMBER");
	 config->multiprocesamiento = config_get_int_value(configuracion, "MULTIPROCESAMIENTO");
 }

 void prepararGossiping(){ //Hace las configuraciones iniciales del gossiping
 	memoriasConocidas = list_create();//memoriasConocidas
 	semillas= list_create();
 	int i=0;
 	while(config->ipSeeds[i]!=NULL){//AGREGA SEMILLAS A LA LISTA DE SEMILLAS
 		int puerto= atoi(config->puertoSeeds[i]);
 		memorias *nueva=crearMemoria(config->ipSeeds[i],puerto,-1,0);
 		list_add(semillas,nueva);
 		i++;
 	}
 	//Me agrego a la lista de memorias activas
 	memorias *yo=crearMemoria(config->ip,config->puerto,config->id,1);
 	list_add(memoriasConocidas,yo);
 }


 segmento *crearSegmento(char* nombre){
	segmento *nuevoSegmento = malloc(sizeof(segmento));
	nuevoSegmento->nombreTabla = malloc(strlen(nombre)+1);
	strcpy(nuevoSegmento->nombreTabla,nombre);
	nuevoSegmento->tablaPaginas = list_create();
	pthread_mutex_init(&nuevoSegmento->lockSegmento, NULL);
	log_info(logger, "Se creo el segmento %s", nombre);

	return nuevoSegmento;
}

 pagina *crearPagina(){
	pagina *pag = malloc(sizeof(pagina));

	pthread_mutex_lock(&lockTablaMarcos);
	pag->nroMarco = getMarcoLibre();
	pthread_mutex_unlock(&lockTablaMarcos);

	pag->modificado = 0;
	return pag;
}

 void agregarPagina(segmento *seg, pagina *pag){

	list_add(seg->tablaPaginas, pag);
	log_info(logger, "Se agrego una página al segmento %s", seg->nombreTabla);

}

 segmento *buscarSegmento(char* nombre){

	int tieneMismoNombre(segmento *seg){
		int rta = 0;
		if(strcmp((seg->nombreTabla), nombre) ==0){
			rta = 1;
		}
		return rta;
	}
	return list_find(tablaSegmentos, (void *) tieneMismoNombre);
}

 pagina *buscarPaginaConKey(segmento *seg, u_int16_t key){

	int tieneMismaKey(pagina *pag){
			int rta = 0;
			u_int16_t offset = (offsetMarco * (pag->nroMarco)) + sizeof(long);
			u_int16_t keyPag = *(u_int16_t*) (memoria + offset);
			if(keyPag == key){
				rta = 1;
			}

			return rta;
		}

	return list_find(seg->tablaPaginas, (void *) tieneMismaKey);

}

 t_config* read_config() {
	return config_create(pathConfig);
}

 t_log* init_logger() {
	return log_create("memoryPool.log", "memoryPool", 1, LOG_LEVEL_INFO);
}


 //-----------------------------------------------------------//
 //---------------------AUXILIARES DE HILOS------------------//
 //---------------------------------------------------------//
 void* recibirOperacion(int cli){
 	//log_info(logger,"sock: %d",cli);

 	char *buffer = malloc(sizeof(char)*2);
 	recvData(cli, buffer, sizeof(char));
 	buffer[1] = '\0';
 	//log_info(logger, "Operacion %d", atoi(buffer));

 	int operacion = atoi(buffer);
 	free(buffer);
 	char** desempaquetado=NULL;
 	char* paquete;

 	if(operacion !=5 && operacion !=6 && operacion !=7){
 	 	char* tamanioPaq = malloc(sizeof(char)*4);
 		recvData(cli,tamanioPaq, sizeof(char)*3);
 		tamanioPaq[3]='\0';
 		int tamanio = atoi(tamanioPaq);
 		free(tamanioPaq);

 		if(tamanio!=0){
 			paquete = malloc(tamanio + sizeof(char));
 			recvData(cli, paquete, tamanio);

 			desempaquetado = string_n_split(paquete, 5, ";");
 	 		free(paquete);
 		}
 		else{
 			desempaquetado=string_n_split("global; ",2,";");
 		}
 	}

 	char* nombreTabla;
 	u_int16_t key;
 	char* value;
 	char* consistencia;
 	int particiones;
 	long tiempoCompactacion;
 	int resp;
 	char* rta;
 	char* rtaFull;

 	switch (operacion) {
 				case 0: //SELECT
 	 					nombreTabla = desempaquetado[0];
 	 					key = atoi(desempaquetado[1]);
 	 					log_info(logger,"SELECT %s %i",nombreTabla,key);
 	 					rta = mSelect(nombreTabla, key);
 	 					//si no existe
 	 					if(strcmp(rta, "2")!=0){
 	 						if(strcmp(rta, "3")==0){
 	 							sendData(cli, rta, strlen(rta)+1);
 	 							log_info(logger,"mande que no existe");
 	 							free(rta);
 	 						}else{
 	 							char* msj;
 	 							msj = empaquetar(0, rta);
 	 							sendData(cli, msj, strlen(msj)+1);
 	 							log_info(logger,"mande que existe rta:%s",msj);
 	 							free(msj);
 	 							free(rta);
 	 						}
 	 					}
 	 					//si estoy full
 	 					else{
 	 						sendData(cli, rta, strlen(rta)+1);
 	 						log_info(logger,"mande que estoy full");
 	 						free(rta);
 	 						rtaFull= malloc(2);
 	 						recvData(cli, rtaFull, sizeof(char));
 	 						rtaFull[1]='\0';
 	 					 	if(atoi(rtaFull)==5){
 	 					 		int r =mJournal();
 	 					 		if(r==0){
 	 					 			sendData(cli,"0",2);
 	 					 			log_info(logger,"mande fin journal");
 	 					 		}else{
 	 					 			sendData(cli,"1",2);
 	 					 			log_info(logger,"mande fin journal");
 	 					 		}
 	 					 	}
 	 					 	free(rtaFull);
 	 					}
 	 					break;

 				case 1: //INSERT
 					usleep(config->retardoMem);

 					nombreTabla = desempaquetado[0];
 					key = atoi(desempaquetado[1]);
 					value = desempaquetado[2];
 					log_info(logger,"INSERT %s %i %s",nombreTabla,key,value);
 					resp = mInsert(nombreTabla, key, value);
 					rta = string_itoa(resp);
 					sendData(cli, rta, sizeof(char)*2);
 					log_info(logger, "Respondi el insert %d\n", resp);
 					free(rta);
 					rtaFull= malloc(2);

 					if(resp == 2){
 						recvData(cli, rtaFull, sizeof(char));
 						rtaFull[1]='\0';
 						if(atoi(rtaFull)==5){
 							mJournal();
 							log_info(logger,"empiezo el insert despues del journal");
 							resp = mInsert(nombreTabla, key, value);
 							rta = string_itoa(resp);
 							sendData(cli, rta, sizeof(char)*2);
 							free(rta);
 							log_info(logger, "envie la respuesta del insert %d\n", resp);
 							}

 						}
 					free(rtaFull);

 					break;

 				case 2: //CREATE
 					usleep(config->retardoMem);

 					nombreTabla = desempaquetado[0];
 					consistencia = desempaquetado[1];
 					particiones = atoi(desempaquetado[2]);
 					tiempoCompactacion = atol(desempaquetado[3]);
 					log_info(logger,"CREATE %s %s %i %ld",nombreTabla, consistencia, particiones, tiempoCompactacion);
 					resp = mCreate(nombreTabla, consistencia, particiones, tiempoCompactacion);
 					rta = string_itoa(resp);
 					sendData(cli, rta, sizeof(char)*2);
 					log_info(logger,"Envie la respuesta del create");
 					free(rta);
 					break;

 				case 3: //DESCRIBE
 					usleep(config->retardoMem);

 					nombreTabla = desempaquetado[0];
 					log_info(logger,"DESCRIBE %s",nombreTabla);
 					rta =mDescribe(nombreTabla);
 					sendData(cli, rta, strlen(rta)+1);
 					log_info(logger,"Envie la respuesta del describe");
 					free(rta);
 					break;

 				case 4: //DROP
 					usleep(config->retardoMem);

 					nombreTabla = desempaquetado[0];
 					log_info(logger,"DROP %s",nombreTabla);
 					resp = mDrop(nombreTabla);
 					rta = string_itoa(resp);
 					sendData(cli, rta, sizeof(char)*2);
 					log_info(logger,"Envie la respuesta del drop");
 					free(rta);

 					break;

 				case 5: //JOURNAL
 					resp = mJournal();
 					rta = string_itoa(resp);
 					sendData(cli, rta, sizeof(char)*2);
 					log_info(logger,"Mande respuesta del journal");
 					free(rta);
 					break;

 				case 6: //DEVOLVER TABLA DE ACTIVOS
 				 	usleep(config->retardoMem);
 				 	sem_wait(&lockTablaMem);
 				 	char *paquete =paqueteVerdadero();
 				 	sem_post(&lockTablaMem);
 				 	sendData(cli, paquete, strlen(paquete)+1);
 				 	free(paquete);
 				 	break;

 				case 7: //RECIBIR TABLA DE ACTIVOS DE LA MEMORIA QUE PIDIO CONFIRMACION
 					usleep(config->retardoMem);
 					recibirMemorias(cli);
 					break;

 				}
 	close(cli);
 	if(desempaquetado!=NULL){
 		liberarSubstrings(desempaquetado);
 	}
 	return NULL;
 }

 void* gestionarConexiones (void* arg){

 	u_int16_t server;
 	int servidorCreado = createServer(config->ip, config->puerto, &server);

 	if(servidorCreado!=0){
 		log_error(logger, "No se pudo crear el servidor");
 		return NULL;
 	}

 	log_info(logger, "Servidor creado exitosamente");
 	listen(server,100000000);
 	log_info(logger, "Servidor escuchando\n");

 	while(1){
 		u_int16_t cliente;
 		int salioBien = acceptConexion(server, &cliente, 0);
 		if(salioBien == 0){
 			log_info(logger, "Recibí una conexión");
 			pthread_t atiendeCliente;
 			pthread_create(&atiendeCliente, NULL, recibirOperacion, (void*)cliente);
 			pthread_detach(atiendeCliente);
 		}
 	}

 	return NULL;
 }

 void* correrInotify(void*arg){

 	while(1){
 		char buffer[BUF_LEN];
 		int file_descriptor = inotify_init();
 		if (file_descriptor < 0) {
 			perror("inotify_init");
 		}
 		int watch_descriptor = inotify_add_watch(file_descriptor, pathConfig, IN_MODIFY );
 		int length = read(file_descriptor, buffer, BUF_LEN);
 		if (length < 0) {
 			perror("read");
 		}
 		modificarConfig();
 	}

 	return NULL;
 }

 void modificarConfig(){
 	t_config *configInotify;
 	pthread_mutex_lock(&lockConfig);
 	configInotify = read_config();
 	config->retardoJournal = config_get_int_value(configInotify,"RETARDO_JOURNAL")*1000;
 	config->retardoGossiping = config_get_int_value(configInotify, "RETARDO_GOSSIPING")*1000;
 	log_info(logger, "Se modificó la config");
 	config_destroy(configInotify);
 	pthread_mutex_unlock(&lockConfig);

 }

 int esNumero(char *num){ //devuelve 0 si es un num
 	int r=0,i=0;
 	if(num!=NULL){
 		while(i<strlen(num)){
 		if(isdigit(num[i])==0){
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

 void* consola(void* arg){

	char* linea;
	while(1){
		if(fin){
			pthread_exit(NULL);
		}
		linea = readline(">");

 		if(!strncmp(linea,"SELECT",6)){
 			char **subStrings= string_n_split(linea,3," ");
 			if(verificarParametros(subStrings, 3)==0){
				if(esNumero(subStrings[2])==0){
					u_int16_t k=atoi(subStrings[2]);
					mSelect(subStrings[1],k);
				}
				else{
					log_error(logger, "La key debe ser un número");
				}
 			}
 			else{
 				log_error(logger, "Faltan parametros");
 			}

 			liberarSubstrings(subStrings);
 		}

 		else if(!strncmp(linea,"INSERT", 6)){
 	 		char **split= string_n_split(linea,4," ");
 			if(verificarParametros(split, 2)==0){
				if(esNumero(split[2])==0){
				int key= atoi(split[2]);
				mInsert(split[1],key,split[3]);
				}
				else{
					log_error(logger, "La key debe ser un número");
				}
 	 		}
 	 		else{
 	 			log_error(logger, "Faltan parametros");
 	 		}

 	 		liberarSubstrings(split);
 	 	}

 	 	else if(!strncmp(linea,"CREATE",6)){
 			char **subStrings= string_n_split(linea,5," ");
 			if(verificarParametros(subStrings, 4)==0){
				if(strcmp(subStrings[2], "SC")==0 ||strcmp(subStrings[2], "EC") == 0 || strcmp(subStrings[2], "SHC")==0) {
					if(esNumero(subStrings[3]) == 0){
						u_int16_t particiones=atoi(subStrings[3]);
						if(esNumero(subStrings[4])==0){
							long timeCompaction=atol(subStrings[4]);
							 mCreate(subStrings[1],subStrings[2],particiones,timeCompaction);
							 log_info(logger,"Se hizo CREATE de la tabla: %s.",subStrings[1]);
						}
						else{
							log_error(logger, "El tiempo de compactacion debe ser un numero");
						}

					}
					else{
						log_error(logger, "El nro de particiones debe ser un numero");
					}
				}else{
					log_error(logger, "El criterio no es valido");
				}
 			}
 			else{
 				log_error(logger, "Faltan parametros");
 			}
 			liberarSubstrings(subStrings);
 		}

 	 	else if(!strncmp(linea,"DESCRIBE",8)){
 			char **subStrings= string_n_split(linea,2," ");
 			if(subStrings[1]==NULL){
 				mDescribe("global");
 			}
 			else{
 				mDescribe(subStrings[1]);
 			}

 			liberarSubstrings(subStrings);
 		}

 	 	else if(!strncmp(linea,"DROP",4)){
 			char **subStrings= string_n_split(linea,2," ");

 			if(subStrings[1]==NULL){
 				log_error(logger,"No se ingreso el nombre de la tabla.");
 			}else{
 				mDrop(subStrings[1]);
 			 	log_info(logger,"Se envio el drop a LFS y se borro de memoria la tabla %s");

 			}

 			free(subStrings[0]);
 			free(subStrings[1]);
 			free(subStrings);
 		}

 	 	else if(!strncmp(linea,"JOURNAL",7 )){
 			mJournal();
 		}

 	 	else if(!strncmp(linea,"MOSTRAR",7)){
 			mostrarMemoria();
 		}
		
 	 	else if(!strncmp(linea,"FULL",4)){
			int resultado=0;
 			resultado=memoriaFull();
			if(resultado)log_info(logger,"La memoria esta FULL");
			if(!resultado)log_info(logger,"La memoria NO esta FULL");
 		}
 	 	else if(!strncmp(linea,"GOSSIP",6)){
					mGossip();
		 		}

 	 	else if(!strncmp(linea,"EXIT",5)){
 			free(linea);
 			fin = 1;
 			break;
 		}
 	 	else{
 	 		log_error(logger, "Sintaxis invalida, ingrese todo en mayusculas y separado por espacios");
 	 		log_error(logger, "Los comandos son SELECT, INSERT, CREATE, DROP, DESCRIBE, JOURNAL y GOSSIP");
 	 	}


 		free(linea);
 	}

 	return NULL;
 }

 void* journalProgramado(void *arg){
	int retardoJ;
 	while(1){
 		pthread_mutex_lock(&lockConfig);
 		retardoJ = config->retardoJournal;
 		pthread_mutex_unlock(&lockConfig);
 		usleep(retardoJ);
 		log_info(logger,"Se realiza un journal programado");
 		mJournal();

 		}

 	return NULL;
 }

 void* gossipProgramado(void* arg){
	prepararGossiping();
	int retardoGoss;
	while(1){
		log_info(logger, "Se realiza un gossip programado");
	 	if(list_is_empty(semillas)){
	 		log_info(logger,"Esta memoria no tiene semillas");
 		}else{
	 	 	mGossip();
	 	}
	 	sem_wait(&lockTablaMem);
	 	mostrarActivas();
	 	sem_post(&lockTablaMem);
	 	pthread_mutex_lock(&lockConfig);
	 	retardoGoss = config->retardoGossiping;
	 	pthread_mutex_unlock(&lockConfig);
	 	usleep(retardoGoss);
	}

 	return NULL;
 }

 //-----------------------------------------------------------//
 //------------------AUXILIARES DE LFS/KERNEL----------------//
 //---------------------------------------------------------//


 char* empaquetar(int operacion, char* paquete){

	 char* msj;
	 int tamanioPaquete = strlen(paquete)+1;
	 char* tamanioFormateado;
	 if(tamanioPaquete<10){
		 	tamanioFormateado = string_from_format("%i00%i", operacion, tamanioPaquete);
		 }
	 if(tamanioPaquete>=10 && tamanioPaquete<100){
	 	tamanioFormateado = string_from_format("%i0%i", operacion, tamanioPaquete);
	 }
	 if(tamanioPaquete>=100){
		tamanioFormateado = string_from_format("%i%i", operacion, tamanioPaquete);

	 }

	 msj = string_from_format("%s%s", tamanioFormateado, paquete);
	 free(tamanioFormateado);
 	 return msj;

 }

 char* formatearSelect(char* nombreTabla, u_int16_t key){
	char* k = string_itoa(key);
	char* paquete = string_from_format("%s;%s", nombreTabla, k);
	free(k);
	return paquete;
}
 char* formatearInsert(char* nombreTabla, long timestamp, u_int16_t key, char* value){
	char* k = string_itoa(key);
	char* tim = string_itoa(timestamp);
	char* paquete = string_from_format("%s;%s;%s;%s", nombreTabla, k, value, tim);
	free(k);
	free(tim);
	return paquete;
}

 char* formatearCreate(char* nombreTabla, char* consistencia, int particiones, long tiempoCompactacion){
	char* part = string_itoa(particiones);
	char* tcomp = string_itoa(tiempoCompactacion);

	char* paquete= string_from_format("%s;%s;%s;%s", nombreTabla, consistencia, part, tcomp);
	free(part);
	free(tcomp);
	return paquete;
}



 u_int16_t handshakeConLissandra(char* ipLissandra,u_int16_t puertoLissandra){
	u_int16_t lfsCliente;
 	int conexionExitosa;
 	int id = 1;
 	conexionExitosa = linkClient(&lfsCliente, ipLissandra , puertoLissandra,id);

 	if(conexionExitosa ==1){
 		log_error(logger, "No se pudo establecer una conexión con LFS, abortando");
 		return 1;
 	}

 	sendData(lfsCliente, "6", sizeof(char) );
 	char* buffer = malloc(sizeof(char)*4);
 	recvData(lfsCliente, buffer, sizeof(char)*3);
 	buffer[3] = '\0';
 	u_int16_t maxV = atoi(buffer);
 	free(buffer);
 	return maxV;
 }


 int crearConexionLFS(){
 	//crea un socket para comunicarse con lfs, devuelve el file descriptor conectado

 	usleep(config->retardoFS);

 	u_int16_t lfsServer;
 	int rta = linkClient(&lfsServer, config->ipFS, config->puertoFS, 1);
 	int i=0;
 	while(rta!=0 && i<5){
 		log_error(logger, "No se pudo crear una conexión con LFS, volverá a intentarse");
 	 	rta = linkClient(&lfsServer, config->ipFS, config->puertoFS, 1);
 	 	i++;
 	}

 	log_info(logger, "Se creó una conexión con LFS");

 	return lfsServer;
 }

 char* selectLissandra(char* nombreTabla,u_int16_t key){
	 u_int16_t lfsSock = crearConexionLFS();
	 char* error = string_duplicate("1");
	 if(lfsSock == -1){
		 log_error(logger, "No se pudo conectar con LFS");
		 return error;
	  }
	 free(error);

	 char* datos = formatearSelect(nombreTabla, key);
	 char* paqueteListo = empaquetar(0, datos);
	 char* value;
	 sendData(lfsSock, paqueteListo, strlen(paqueteListo)+1);
	 log_info(logger, "Para Lissandra: SELECT %s %i", nombreTabla,key);
	 free(datos);
	 free(paqueteListo);

	 char* buffer = malloc(sizeof(char)*2);
	 recvData(lfsSock, buffer, sizeof(char));
	 buffer[1] = '\0';
	 if(atoi(buffer)==0){
		 char* tam = malloc(sizeof(char)*4);
		 recvData(lfsSock, tam, sizeof(char)*3);
		 tam[3]= '\0';
		 int t = atoi(tam);

		 log_info(logger, "Tamanio value %d", t);
		 value = malloc(t+sizeof(char));
		 recvData(lfsSock, value, t);
		 log_info(logger, "Se recibio el valor %s", value);
		 free(buffer);
		 free(tam);
	 }
	 else{
		 value = NULL;
		 log_info(logger, "No existe un valor con esa key en LFS");
		 free(buffer);
	 }

	 close(lfsSock);
	 return value;

 }

int insertLissandra(char* nombreTabla, long timestamp, u_int16_t key, char* value){

	u_int16_t lfsSock = crearConexionLFS();
	if(lfsSock == -1){
		log_error(logger, "No se pudo conectar con LFS");
		return 1;
	}
	char* datos = formatearInsert(nombreTabla, timestamp, key, value);
	char* paqueteListo = empaquetar(1, datos);
	sendData(lfsSock, paqueteListo, strlen(paqueteListo)+1);
	log_info(logger, "Paquete INSERT %s %ld %i %s",nombreTabla,timestamp,key,value);
	free(datos);
	free(paqueteListo);

	char *buffer = malloc(sizeof(char)*2);
	recvData(lfsSock, buffer, sizeof(char));
	log_info(logger,"Recibi la respuesta de Liss");
	close(lfsSock);
	buffer[1] = '\0';
	int rta = atoi(buffer);
	free(buffer);
	return rta;
}

char* describeLissandra(char* nombreTabla){

	u_int16_t lfsSock = crearConexionLFS();
	char* error = string_duplicate("1");
	if(lfsSock == -1){
		log_error(logger, "No se pudo conectar con LFS");
		return error;
	}
	free(error);
	char* describeGlobal = string_duplicate("3000");

	if(strcmp(nombreTabla, "global")!=0){
		char* paqueteListo = empaquetar(3, nombreTabla);
		sendData(lfsSock, paqueteListo, strlen(paqueteListo)+1);
		log_info(logger, "Se pidio un describe de %s", nombreTabla);
		free(paqueteListo);
	}
	else{
		sendData(lfsSock, describeGlobal, strlen(describeGlobal)+1);
		log_info(logger, "Se pidio un describe global");

	}
	free(describeGlobal);

	//recibir
	char* buffer = malloc(sizeof(char)*2);
	recvData(lfsSock, buffer, sizeof(char));
	buffer[1]='\0';

	if(atoi(buffer) == 0){
		char* tam = malloc(sizeof(char)*5);
		recvData(lfsSock, tam, sizeof(char)*4);
		tam[4] = '\0';
		int tamanio = atoi(tam);
		char* listaMetadatas = malloc(tamanio+1);
		recvData(lfsSock, listaMetadatas, tamanio);
		log_info(logger, "Respuesta del describe %s", listaMetadatas);
		close(lfsSock);
		free(buffer);
		free(tam);

		return listaMetadatas;
	}
	else{
		return buffer;
	}

}

int createLissandra(char* nombreTabla, char* criterio, u_int16_t nroParticiones, long tiempoCompactacion){

	u_int16_t lfsSock = crearConexionLFS();
	if(lfsSock == -1){
		log_error(logger, "No se pudo conectar con LFS");
		return 1;
	}
	char* datos = formatearCreate(nombreTabla, criterio, nroParticiones, tiempoCompactacion);
	char* paqueteListo = empaquetar(2, datos);
	sendData(lfsSock, paqueteListo, strlen(paqueteListo)+1);
	log_info(logger,"Se pidio un create de %s %s %i %ld",nombreTabla,criterio,nroParticiones,tiempoCompactacion);
	free(datos);
	free(paqueteListo);

	char* buffer = malloc(sizeof(char)*2);
	recvData(lfsSock, buffer, sizeof(char));
	log_info(logger,"Recibi la respuesta del create de Liss");
	buffer[1] = '\0';
	close(lfsSock);
	int rta = atoi(buffer);
	log_info(logger, "Respuesta del create %d", rta);
	free(buffer);
	return rta;
}

int dropLissandra(char* nombreTabla){
	u_int16_t lfsSock = crearConexionLFS();
	if(lfsSock == -1){
		log_error(logger, "No se pudo conectar con LFS");
		return 1;
	}
	char* paqueteListo = empaquetar(4, nombreTabla);
	sendData(lfsSock, paqueteListo, strlen(paqueteListo)+1);
	log_info(logger, "Paquete DROP %s", nombreTabla);
	free(paqueteListo);

	char* buffer = malloc(sizeof(char)*2);
	recvData(lfsSock, buffer, sizeof(char));
	log_info(logger,"Recibi la respuesta del drop de Liss");
	buffer[1] = '\0';
	close(lfsSock);
	int rta = atoi(buffer);
	free(buffer);
	return rta;
}


 //---------------------------------------------------------//
 //------------------MANEJO DE MEMORIA---------------------//
 //-------------------------------------------------------//

 void agregarDato(long timestamp, u_int16_t key, char* value, pagina *pag){
	marco *marc = list_get(tablaMarcos, pag->nroMarco);
	pthread_mutex_lock(&marc->lockMarco);
 	int offset = offsetMarco*(pag->nroMarco);
 	memcpy(memoria+offset, &timestamp, sizeof(long));
 	offset = offset + sizeof(long);
 	memcpy(memoria+offset, &key,sizeof(u_int16_t));
 	int offset2 = offset + sizeof(u_int16_t);
 	memcpy(memoria+offset2, value, strlen(value)+1);
 	pthread_mutex_unlock(&marc->lockMarco);
 	log_info(logger, "Se agrego el dato %s al marco %d", value, pag->nroMarco);
 }

 int getMarcoLibre(){
	 int nroMarco;
	 if(!memoriaFull()){
		 if(hayMarcosLibres()){
			log_info(logger, "Hay marcos libres");
			int primerMarcoLibre(marco *unMarco){
				 return unMarco->estaLibre == 0;
			 }
			marco* marc = list_find(tablaMarcos, (void*)primerMarcoLibre);
			marc->estaLibre = 1;
			log_info(logger, "Se le asigno a la pagina el marco %i", marc->nroMarco);
			return marc->nroMarco;
		 }
		 else{
			 log_info(logger, "No hay marcos libres");
			 nroMarco = mlru();
			 return nroMarco;
		 }
	 }
	 else{
		 return -1;
	 }
 }


 void liberarMarco(int nroMarcoALiberar){
 	marco* nuevo = list_get(tablaMarcos,nroMarcoALiberar);
 	pthread_mutex_lock(&nuevo->lockMarco);
 	nuevo->estaLibre = 0;
 	pthread_mutex_unlock(&nuevo->lockMarco);

 }

 void agregarAReemplazables(marco *unMarco){
	 bool estaEnLaLista(marco *otroMarco){
		 return unMarco->nroMarco == otroMarco->nroMarco;

	 }
	 if(!list_any_satisfy(marcosReemplazables, (void*)estaEnLaLista)){
		 list_add(marcosReemplazables, unMarco);
	 }
	 actualizarLista();
 }

 void actualizarLista(){
	 bool marcoMasViejo(marco* unMarco, marco* marcoViejo){
		 if(unMarco->ultimoUso == marcoViejo->ultimoUso){
			 return unMarco->nroMarco < marcoViejo->nroMarco;
		 }
		 return unMarco->ultimoUso < marcoViejo->ultimoUso;
	 }

	 list_sort(marcosReemplazables, (void*) marcoMasViejo);
 }



 void eliminarDeReemplazables(int nroMarco){

	 bool mismoMarco(marco *marco){
		 return marco->nroMarco == nroMarco;
	  }

	 list_remove_by_condition(marcosReemplazables, (void*) mismoMarco);
 }

 int hayMarcosLibres(){
	 bool algunoLibre(marco* unMarco){
		 return unMarco->estaLibre == 0;
	 }
	 return list_any_satisfy(tablaMarcos,(void*)algunoLibre);
}

 int memoriaFull(){
	 if(hayMarcosLibres()==0){
		 if(list_is_empty(marcosReemplazables)){
			 log_info(logger, "Memoria full");
		 return 1;
		 }
		 return 0;
	 }
	 return 0;
 }

 int mlru(){
	 if(!list_is_empty(marcosReemplazables)){
		 log_info(logger, "Se ingreso al LRU");
		 marco *marc = list_get(marcosReemplazables, 0);
		 log_info(logger, "El marco que se va a reemplazar es el %i", marc->nroMarco);
		 return marc->nroMarco;
	 }
	 else{
		 return -1;
	 }
 }


 bool estaModificada(pagina *pag){
 	bool res = false;
 	if(pag->modificado == 1){
 		res = true;
 	}

 	return res;
 }
 //---------------------------------------------------------//
 //-----------------AUXILIARES SECUNDARIAS-----------------//
 //-------------------------------------------------------//


void mostrarMemoria(){
	int desplazador=0 ,i=0;
	while(i<cantMarcos){
		printf("Timestamp: %ld \n", *(long*)((memoria) + desplazador));
		printf("Key: %d \n", *(u_int16_t*)((memoria)+ sizeof(long) + desplazador));
		printf("Value: %s \n", (char*)((memoria) + sizeof(long) + sizeof(u_int16_t)+desplazador));
		desplazador += offsetMarco;
		i++;
	}
}

void* conseguirValor(pagina* pNueva){
	void* value = malloc(maxValue);
	memcpy(value, ((memoria) + sizeof(long) + sizeof(u_int16_t)+ (pNueva->nroMarco)*offsetMarco),maxValue);
	return value;
}

void *conseguirTimestamp(pagina *pag){
	void* timestamp = malloc(sizeof(long));
	memcpy(timestamp,((memoria) + offsetMarco*pag->nroMarco),sizeof(long));
	return timestamp;
}

void *conseguirKey(pagina *pag){
	void* key = malloc(sizeof(u_int16_t));
	memcpy(key,((memoria) + sizeof(long) + offsetMarco*pag->nroMarco),sizeof(u_int16_t));
	return key;
}


//---------------------------------------------------------//
//--------------------------BORRADO-----------------------//
//-------------------------------------------------------//


void eliminarSegmento(segmento* nuevo){
	bool mismoNombre(segmento *seg){
		return string_equals_ignore_case(seg->nombreTabla, nuevo->nombreTabla);
	}

	list_remove_and_destroy_by_condition(tablaSegmentos, (void*)mismoNombre ,(void*)segmentoDestroy);
}

void paginaDestroy(pagina* pagParaDestruir){
	if((pagParaDestruir->modificado)==0){
	  	eliminarDeReemplazables(pagParaDestruir->nroMarco);
	 }
	liberarMarco(pagParaDestruir->nroMarco);
	free(pagParaDestruir);
}

void segmentoDestroy(segmento* segParaDestruir){
   list_destroy_and_destroy_elements(segParaDestruir->tablaPaginas,(void*)paginaDestroy);
   free(segParaDestruir->nombreTabla);
   pthread_mutex_destroy(&segParaDestruir->lockSegmento);
   free(segParaDestruir);
}

void eliminarMarcos(){
	list_destroy_and_destroy_elements(tablaMarcos,(void*)marcoDestroy);
	list_destroy(marcosReemplazables);

}

void marcoDestroy(marco *unMarco){
	pthread_mutex_destroy(&unMarco->lockMarco);
	free(unMarco);
}

void liberarSubstrings(char **liberar){
	int i=1;
	while(liberar[i-1]!=NULL){
		free(liberar[i-1]);
		i++;
	}
	free(liberar);
}
void destruirConfig(){
	free(config->ip);
	free(config->ipFS);
	liberarSubstrings(config->ipSeeds);
	liberarSubstrings(config->puertoSeeds);
	free(config);

}

void finalizar(){
	list_destroy_and_destroy_elements(tablaSegmentos, (void*)segmentoDestroy);
	eliminarMarcos();
	pthread_mutex_destroy(&lockTablaSeg);
	pthread_mutex_destroy(&lockTablaMarcos);
	pthread_mutex_destroy(&lockConfig);
	pthread_mutex_destroy(&lockLRU);
	pthread_mutex_destroy(&lockJournal);
	sem_destroy(&semJournal);
	sem_destroy(&lockTablaMem);
	free(memoria);
	destruirConfig();
	free(pathConfig);
	log_info(logger, "Memoria limpia, adiós mundo cruel");
	log_destroy(logger);
}


//-------------------------------------//
//---------------API------------------//
//-----------------------------------//


int mInsert(char* nombreTabla, u_int16_t key, char* valor){

	sem_wait(&semJournal);
	if(strlen(valor)+1 >maxValue){
		log_info(logger, "Se intentó insertar un value mayor al tamaño permitido");
		sem_post(&semJournal);
		return 1;
	}
	log_info(logger, "Empieza el insert");
	segmento *seg = buscarSegmento(nombreTabla);
	pagina *pag;
	long timestampActual;

	if(seg != NULL){ //existe el segmento
		log_info(logger,"existe segmento ");
		pthread_mutex_lock(&seg->lockSegmento);
		pag = buscarPaginaConKey(seg, key);

		if (pag == NULL){ //no existe la pagina
			pthread_mutex_lock(&lockLRU);
			log_info(logger,"no existe pagina ");
			pag = crearPagina();
			if(pag->nroMarco==-1){//estoy full
				pthread_mutex_unlock(&seg->lockSegmento);
				pthread_mutex_unlock(&lockLRU);
				sem_post(&semJournal);
				free(pag);
				return 2;
			}
			eliminarDeReemplazables(pag->nroMarco);
			pthread_mutex_unlock(&lockLRU);
			agregarPagina(seg,pag);
			timestampActual = time(NULL);
			agregarDato(timestampActual, key, valor, pag);
			pag->modificado = 1;

		}else{ //existe la pagina
			pthread_mutex_lock(&lockLRU);
			log_info(logger,"existe pagina ");
			if(pag->modificado==0){
				eliminarDeReemplazables(pag->nroMarco);
			}
			pthread_mutex_unlock(&lockLRU);
			agregarDato(time(NULL),key,valor,pag);
			pag->modificado = 1;

		}
		pthread_mutex_unlock(&seg->lockSegmento);

	}else{ //no existe el segmento
		pthread_mutex_lock(&lockLRU);
		log_info(logger,"no existe segmento ");
		pagina *pag = crearPagina();
		if(pag->nroMarco==-1){//estoy full
			sem_post(&semJournal);
			pthread_mutex_unlock(&lockLRU);
			free(pag);
			return 2;
		}
		eliminarDeReemplazables(pag->nroMarco);
		pthread_mutex_unlock(&lockLRU);

		pthread_mutex_lock(&lockTablaSeg);
		seg = crearSegmento(nombreTabla);
		list_add(tablaSegmentos, seg);
		pthread_mutex_unlock(&lockTablaSeg);
		pthread_mutex_lock(&seg->lockSegmento);
		agregarPagina(seg,pag);
		timestampActual = time(NULL);
		agregarDato(timestampActual, key, valor, pag);
		pag->modificado = 1;
		pthread_mutex_unlock(&seg->lockSegmento);
	}
	log_info(logger, "Se inserto al segmento %s el valor %s", nombreTabla, valor);
	sem_post(&semJournal);
	log_info(logger, "Termino el insert");
	return 0;
}



char* mSelect(char* nombreTabla,u_int16_t key){

	log_info(logger, "Empieza el select");
	sem_wait(&semJournal);
	segmento *nuevo = buscarSegmento(nombreTabla);
	pagina* pNueva;
	char* valorPagNueva;
	char* valor;
	char* noExiste =string_duplicate("3");
	char* estoyFull = string_duplicate("2");

	//log_info(logger, "Se pidio un select de la tabla %s key %d", nombreTabla, key);
	if(nuevo!= NULL){
		pNueva = buscarPaginaConKey(nuevo,key);

		if(pNueva != NULL){//encontro segmento y pagina
			pthread_mutex_lock(&lockLRU);
			if(pNueva->modificado == 0){
				marco *marc = list_get(tablaMarcos, pNueva->nroMarco);
				pthread_mutex_lock(&marc->lockMarco);
				marc->ultimoUso = time(NULL);
				pthread_mutex_unlock(&marc->lockMarco);
				actualizarLista();
			}
			pthread_mutex_unlock(&lockLRU);

			valor = (char*)conseguirValor(pNueva);
			log_info(logger, "Se seleccionó el valor %s", valor);
			log_info(logger, "Encontro segmento y pagina");

			sem_post(&semJournal);
			free(estoyFull);
			free(noExiste);
			return valor;
		}
		else{//si encontro el segmento pero no la página
			pthread_mutex_lock(&lockLRU);
			pNueva = crearPagina();
			if(pNueva->nroMarco==-1){//si estoy full
				sem_post(&semJournal);
				pthread_mutex_unlock(&lockLRU);
				free(pNueva);
				free(noExiste);
				return estoyFull;

			}//si no estoy full
			marco *marc = list_get(tablaMarcos, pNueva->nroMarco);
			pthread_mutex_lock(&marc->lockMarco);
			marc->ultimoUso = time(NULL);
			pthread_mutex_unlock(&marc->lockMarco);
			agregarAReemplazables(marc);
			pthread_mutex_unlock(&lockLRU);

			valorPagNueva = selectLissandra(nombreTabla,key);
			if(valorPagNueva != NULL){//si lfs tenía el valor
				pthread_mutex_lock(&nuevo->lockSegmento);
				agregarPagina(nuevo,pNueva);
				pthread_mutex_unlock(&nuevo->lockSegmento);
				agregarDato(time(NULL),key,valorPagNueva,pNueva);

				log_info(logger, "Se seleccionó el valor %s", valorPagNueva);
				sem_post(&semJournal);
				free(estoyFull);
				free(noExiste);
				return valorPagNueva;
			}
			else{//si lfs no tenía el valor
				sem_post(&semJournal);
				pthread_mutex_lock(&lockLRU);
				paginaDestroy(pNueva);
				pthread_mutex_unlock(&lockLRU);
				free(estoyFull);
				return noExiste;
			}

		}
	}
	else{//si no encontró el segmento
		pthread_mutex_lock(&lockLRU);
		pNueva = crearPagina();
		if(pNueva->nroMarco==-1){//si estoy full
			sem_post(&semJournal);
			pthread_mutex_unlock(&lockLRU);
			free(pNueva);
			free(noExiste);
			return estoyFull;
		}
		marco *marc = list_get(tablaMarcos, pNueva->nroMarco);
		pthread_mutex_lock(&marc->lockMarco);
		marc->ultimoUso = time(NULL);
		pthread_mutex_unlock(&marc->lockMarco);
		agregarAReemplazables(marc);
		pthread_mutex_unlock(&lockLRU);
		pthread_mutex_lock(&lockTablaSeg);
		nuevo = crearSegmento(nombreTabla);
		list_add(tablaSegmentos, nuevo);
		pthread_mutex_unlock(&lockTablaSeg);

		valorPagNueva = selectLissandra(nombreTabla,key);
		if(valorPagNueva !=NULL){//si lfs tenia el valor
			pthread_mutex_lock(&nuevo->lockSegmento);
			agregarPagina(nuevo,pNueva);
			pthread_mutex_unlock(&nuevo->lockSegmento);

			agregarDato(time(NULL),key,valorPagNueva,pNueva);
			log_info(logger, "Se seleccionó el valor %s", valorPagNueva);
			sem_post(&semJournal);
			free(estoyFull);
			free(noExiste);
			return valorPagNueva;

		}
		else{//lfs no tenía el valor
			sem_post(&semJournal);
			pthread_mutex_lock(&lockLRU);
			paginaDestroy(pNueva);
			pthread_mutex_unlock(&lockLRU);

			free(estoyFull);
			return noExiste;
		}

	}
}


int mCreate(char* nombreTabla, char* criterio, u_int16_t nroParticiones, long tiempoCompactacion){

	int rta = createLissandra(nombreTabla,criterio,nroParticiones,tiempoCompactacion);
	log_info(logger, "La respuesta del create fue %d", rta);

	return rta;
}


char* mDescribe(char* nombreTabla){

	char* rta = describeLissandra(nombreTabla);
	if(string_equals_ignore_case(rta, "1")){
		return rta;
	}
	else{
		char* respuesta = string_from_format("0%s", rta);
		free(rta);
		return respuesta;
	}
}

int mDrop(char* nombreTabla){

	sem_wait(&semJournal);

	segmento* nuevo = buscarSegmento(nombreTabla);

	if(nuevo != NULL){
		pthread_mutex_lock(&lockTablaSeg);
		pthread_mutex_lock(&lockLRU);
		eliminarSegmento(nuevo);
		pthread_mutex_unlock(&lockLRU);
		pthread_mutex_unlock(&lockTablaSeg);
		log_info(logger, "Se realizo un drop del segmento %s", nombreTabla);

	}
	sem_post(&semJournal);
	int rta = dropLissandra(nombreTabla);

	return rta;
}


int mJournal(){
	pthread_mutex_lock(&lockJournal);
	if(list_is_empty(tablaSegmentos)){
		log_info(logger, "La memoria ya esta vacia");
		pthread_mutex_unlock(&lockJournal);
		return 0;
	}
	for(int i = 0; i<config->multiprocesamiento; i++){
		sem_wait(&semJournal);
	}
	log_info(logger,"Empieza el journal");
	for(int i =0; i<(tablaSegmentos->elements_count); i++){
		segmento *seg = list_get(tablaSegmentos, i);
		char* nombreSegmento = string_duplicate(seg->nombreTabla);
		t_list *paginasMod;
		if(list_is_empty(seg->tablaPaginas)){
			free(nombreSegmento);
			break;
		}
		if(list_any_satisfy(seg->tablaPaginas, (void*)estaModificada)){
			paginasMod = list_filter(seg->tablaPaginas, (void*)estaModificada);

			for(int j=0; j<(paginasMod->elements_count); j++){
				pagina *pag = list_get(paginasMod, j);
				void* t = conseguirTimestamp(pag);
				long timestamp = *(long*)t;
				void* k = conseguirKey(pag);
				u_int16_t key = *(u_int16_t*) k;
				char* value = (char*)conseguirValor(pag);
				int insertExitoso = insertLissandra(nombreSegmento, timestamp, key, value);
				log_info(logger, "Rta insert LFS %d", insertExitoso);

				if(insertExitoso == 0){
					log_info(logger, "Se insertó correctamente el valor %s en la tabla %s", value, nombreSegmento);
				}
				else{
					log_info(logger, "No se pudo insertar el valor %s en la tabla %s", value, nombreSegmento);
				}
				free(t);
				free(k);
				free(value);
			}
			log_info(logger, "Se borra la lista de modificados para el segmento %s", nombreSegmento);
			list_destroy(paginasMod);
			free(nombreSegmento);
		}else{
			free(nombreSegmento);
		}

	}

	log_info(logger, "Fin del journal, procede a borrar datos existentes");
	list_clean_and_destroy_elements(tablaSegmentos, (void*)segmentoDestroy);
	list_clean(marcosReemplazables);
	int listaVacia = list_is_empty(tablaSegmentos);
	if (listaVacia == 1){
		marco *marc;
		for(int i =0; i<tablaMarcos->elements_count; i++){
			marc = list_get(tablaMarcos, i);
			log_info(logger, "Marco %i, libre: %i", marc->nroMarco, marc->estaLibre);
		}
		log_info(logger, "Datos borrados, se desbloquea la tabla de segmentos");


		void itera(marco *marco){
			marco->estaLibre=0;
		}
		list_iterate(tablaMarcos,(void*)itera);


		for(int i = 0; i<config->multiprocesamiento; i++){
			sem_post(&semJournal);

		}
		pthread_mutex_unlock(&lockJournal);
		log_info(logger,"Termine definitivamente el journal");

		return 0;
	}
	else{
		log_error(logger, "Algo salió mal al vaciar la lista de segmentos");
		return 1;
	}

}

bool existeMemoria(char *ip,int puerto,int id){
	bool mismoID(memorias *repetida){
		if(id==0){
			if(string_equals_ignore_case(repetida->ip,ip)){
				if(repetida->puerto==puerto){
					return true;
				}else{
					return false;
				}
			}else{
				return false;
			}
		}else{
			return repetida->nroMem==id;
		}
	}
	return list_any_satisfy(memoriasConocidas,(void *)mismoID);
}
memorias *obtenerMemorias(char *ip,int puerto,int id){
	memorias *obtenida=NULL;
	bool mismoID(memorias *repetida){
		if(id==0){
			if(string_equals_ignore_case(repetida->ip,ip)){
				if(repetida->puerto == puerto){
					return true;
				}else{
					return false;
				}
			}else{
				return false;
			}
		}else{
			return repetida->nroMem==id;
		}
	}
	obtenida=list_find(memoriasConocidas,(void *)mismoID);
	return obtenida;
}
void desempaquetarMemorias(char* paquete){
	while(paquete!=NULL){
		char **split=string_n_split(paquete,4,";");
		if(verificarParametros(split,3)==0){
			int id=atoi(split[0]);
			int puerto=atoi(split[2]);
			if(existeMemoria(split[1],puerto,id)){
				memorias *obtener=obtenerMemorias(split[1],puerto,id);
				obtener->activa=1;
			}else{
				memorias *nueva=crearMemoria(split[1],puerto,id,1);
				list_add(memoriasConocidas,nueva);
			}
			free(paquete);
			if(split[3]!=NULL){
				paquete=string_duplicate(split[3]);
			}else{
				paquete=NULL;
			}
			liberarSubstrings(split);
		}else{
			break;
		}
	}
}

char *empaquetarMemorias(){
	char *paquete=NULL;
	void empaquetando(memorias *aEmpaquetar){
		char *paquetito=string_from_format("%i;%s;%i",aEmpaquetar->nroMem,aEmpaquetar->ip,aEmpaquetar->puerto);
		if(paquete==NULL){
			paquete=string_duplicate(paquetito);
		}else{
			char *aux=string_duplicate(paquete);
			free(paquete);
			paquete=string_from_format("%s;%s",aux,paquetito);
			free(aux);
		}
		free(paquetito);
	}
	list_iterate(memoriasConocidas,(void *)empaquetando);
	return paquete;
}
char *paqueteVerdadero(){
   char* paquete=empaquetarMemorias();
   char* paqueteListo=empaquetar(7,paquete);
   free(paquete);
   return paqueteListo;
}

int memoriaActiva(char *ip, int puerto){
	u_int16_t cliente;
	int conexion = linkClient(&cliente,ip,puerto,0);
	if(conexion ==1){
		return 0; // no esta activa la memoria
	}
	return cliente;
}
void pedirMemorias(int cliente){
	char* codOpe =string_duplicate("6");
	sendData(cliente,codOpe,strlen(codOpe)+1);
	free(codOpe);
	char *siete=malloc(2);
	recvData(cliente,siete,1);
	free(siete);
	recibirMemorias(cliente);
}
void recibirMemorias(int cliente){
	char* tamanioTabla=malloc(4);
	recvData(cliente,tamanioTabla,3);
	tamanioTabla[3] = '\0';
	char* buffer=malloc(atoi(tamanioTabla)+1);
	recvData(cliente,buffer,atoi(tamanioTabla));
	free(tamanioTabla);
	sem_wait(&lockTablaMem);
	desempaquetarMemorias(buffer);  //aca actualizo mi tabla con lo que me envian
	sem_post(&lockTablaMem);
}

void enviarMemorias(char *ip,int puerto){
	int socket=memoriaActiva(ip,puerto);
	if(socket==0){
		sem_wait(&lockTablaMem);
		desactivarMemoria(ip,puerto);
		sem_post(&lockTablaMem);
		//log_info(logger,"la semilla de ip %s y puerto %i no esta activa",ip,puerto);
	}else{
		sem_wait(&lockTablaMem);
		char *paquete =paqueteVerdadero();
		sem_post(&lockTablaMem);
		sendData(socket,paquete,strlen(paquete)+1);
		free(paquete);
		close(socket);
	}
}
void desactivarMemoria(char *ip,int puerto){
	if(existeMemoria(ip,puerto,0)){//Si existe la desactiva
		memorias *semi=obtenerMemorias(ip,puerto,0);
		semi->activa=0;
	}
}

void mostrarActivas(){
	int i = 0, size;
	size=list_size(memoriasConocidas);
	printf("\n*Tabla Activas: *\n");
	while(i<size){
		memorias* aux = list_get(memoriasConocidas,i);
		printf("ID: %i \n",aux->nroMem);
		printf("IP: %s \n",aux->ip);
		printf("Puerto: %i \n",aux->puerto);
		printf("Activa : %i \n",aux->activa);
		printf("---------- \n");
		i++;
	}
}

memorias *crearMemoria(char *ip,int puerto, int id, int activa){
	memorias *nueva=malloc(sizeof(memorias));
	nueva->activa=activa;
	nueva->ip=string_duplicate(ip);
	nueva->puerto=puerto;
	nueva->nroMem=id;
	return nueva;
}
void liberarMemoria(memorias *victima){
	free(victima->ip);
	free(victima);
}
void mGossip(){
	void gossipGirl(memorias *semilla){
		int socket=memoriaActiva(semilla->ip,semilla->puerto);
		if(socket==0){
			sem_wait(&lockTablaMem);
			desactivarMemoria(semilla->ip,semilla->puerto);
			sem_post(&lockTablaMem);
		}else{
			//agregar las nuevas memorias que recibo
			pedirMemorias(socket);
			close(socket);
			//enviar tabla
			enviarMemorias(semilla->ip,semilla->puerto);
		}
	}
	list_iterate(semillas,(void *)gossipGirl);
}
