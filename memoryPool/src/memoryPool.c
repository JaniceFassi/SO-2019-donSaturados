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


	int exito = inicializar();

	if(exito==1){
		log_error(logger, "Abortando ejecución");
		return 1;

	}


	pthread_t inotify;
	pthread_create(&inotify, NULL, correrInotify, NULL);



	pthread_t gossipTemporal;
	pthread_create(&gossipTemporal, NULL, gossipProgramado, NULL);
	int *fin;
	pthread_t hiloConsola;
	pthread_create(&hiloConsola, NULL, consola, NULL);

	pthread_t gestorConexiones;
	pthread_create(&gestorConexiones, NULL, gestionarConexiones, NULL);


	pthread_t journalTemporal;
	pthread_create(&journalTemporal, NULL, journalProgramado, NULL);


	pthread_join(hiloConsola, (void*)&fin);
	if(fin == 0){
		pthread_cancel(gestorConexiones);
		pthread_cancel(inotify);
		//pthread_cancel(journalTemporal);
		//pthread_cancel(gossipTemporal);
		log_info(logger, "Se apagará la memoria de forma correcta");

	}
	else{
	pthread_join(gestorConexiones, NULL);
	//pthread_join(journalTemporal, NULL);
	//pthread_join(gossipTemporal, NULL);

	}


	finalizar();
	return EXIT_SUCCESS;
}

//---------------------------------------------------------//
//------------------AUXILIARES DE ARRANQUE----------------//
//-------------------------------------------------------//

 int inicializar(){
	logger = init_logger();
	t_config* configuracion = read_config();
	config = malloc(sizeof(estructuraConfig));
	int tamanioMemoria = config_get_int_value(configuracion, "TAM_MEM");
	init_configuracion(configuracion);

	pathConfig = malloc(strlen("/home/utnso/tp-2019-1c-donSaturados/memoryPool/memoryPool.config")+1);
	strcpy(pathConfig, "/home/utnso/tp-2019-1c-donSaturados/memoryPool/memoryPool.config");

	posicionUltimoUso = 0; //Para arrancar la lista de usos en 0, ira aumentando cuando se llene NO TOCAR PLS

	pthread_mutex_init(&lockTablaSeg, NULL);
	pthread_mutex_init(&lockTablaMarcos, NULL);
	pthread_mutex_init(&lockTablaUsos, NULL);
	pthread_mutex_init(&lockConfig, NULL);
	sem_init(&lockTablaMemAct,0,1);
	pthread_mutex_init(&lockTablaMemSec,NULL);
	sem_init(&semJournal, 0, config->multiprocesamiento);

	prepararGossiping(configuracion);


	memoria = calloc(1,tamanioMemoria);

	if(memoria == NULL){
		log_error(logger, "Falló el inicio de la memoria principal, abortando ejecución");
		return 1;
	}
	log_info(logger, "Se inicializo la memoria con tamanio %d", tamanioMemoria);


	u_int16_t lfsServidor;
	maxValue = handshakeConLissandra(lfsServidor, config->ipFS, config->puertoFS);

	//maxValue = 20;

	if(maxValue == 1){
		log_error(logger, "No se pudo recibir el handshake con LFS, abortando ejecución\n");
		return 1;
	}

	log_info(logger, "Tamanio máximo recibido de FS: %d", maxValue);

	offsetMarco = sizeof(long) + sizeof(u_int16_t) + maxValue;
	tablaMarcos = list_create();
	tablaSegmentos = list_create();
	listaDeUsos = list_create();


	cantMarcos = tamanioMemoria/offsetMarco;
	for(int i=0; i<cantMarcos; i++){
			marco* unMarco = malloc(sizeof(marco));
			unMarco->nroMarco = i;
			unMarco->estaLibre = 0;
			pthread_mutex_init(&unMarco->lockMarco, NULL);
			list_add(tablaMarcos, unMarco);
			/*posMarcoUsado *pos = malloc(sizeof(posMarcoUsado));
			pos->nroMarco = i;
			pos->posicionDeUso = 0;
			list_add(listaDeUsos, pos);
		*/}



	config_destroy(configuracion);
	return 0;
}
 void init_configuracion(t_config* configuracion){
	 config->retardoJournal = config_get_int_value(configuracion,"RETARDO_JOURNAL")*1000;
	 config->retardoGossiping= config_get_int_value(configuracion, "RETARDO_GOSSIPING")*1000;
	 config->retardoMem = config_get_int_value(configuracion, "RETARDO_MEM")*1000;
	 config->retardoFS = config_get_int_value(configuracion, "RETARDO_FS")*1000;
	 config->puerto = config_get_int_value(configuracion, "PUERTO");
	 config->ip = malloc(strlen("192.168.0.32")+1);
	 strcpy(config->ip, config_get_string_value(configuracion, "IP"));
	 config->puertoFS= config_get_int_value(configuracion,"PUERTO_FS");
	 config->ipFS = malloc(strlen("192.168.0.32")+1);
	 strcpy(config->ipFS, config_get_string_value(configuracion,"IP_FS"));
	 config->multiprocesamiento = config_get_int_value(configuracion, "MULTIPROCESAMIENTO");

 }

 void prepararGossiping(t_config *configuracion){ //Hace las configuraciones iniciales del gossiping, NO lo empieza solo lo deja configurado
	 
	 tablaMemActivas = list_create();
	 tablaMemActivasSecundaria = list_create();

	 char* ipsConfig = config_get_string_value(configuracion,"IP_SEEDS");
	 char* seedsConfig = config_get_string_value(configuracion,"PUERTO_SEEDS");

	 ipSeeds = string_split(ipsConfig,";");
	 puertoSeeds = string_split(seedsConfig,";");
	 
	 idMemoria = config_get_int_value(configuracion,"MEMORY_NUMBER");//Unico de cada proceso
	 char* ipMem = config_get_string_value(configuracion,"IP");
	 char* puerto = config_get_string_value(configuracion,"PUERTO");

	 agregarMemActiva(idMemoria,ipMem,puerto);
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
	pag->nroMarco = primerMarcoLibre();
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
	return config_create("/home/utnso/tp-2019-1c-donSaturados/memoryPool/memoryPool.config");
}

 t_log* init_logger() {
	return log_create("memoryPool.log", "memoryPool", 1, LOG_LEVEL_INFO);
}


 //-----------------------------------------------------------//
 //---------------------AUXILIARES DE HILOS------------------//
 //---------------------------------------------------------//
 void* recibirOperacion(int cli){
 	log_info(logger,"sock: %d",cli);

 	char *buffer = malloc(sizeof(char)*2);
 	int b = recvData(cli, buffer, sizeof(char));
 	buffer[1] = '\0';
 	log_info(logger, "Bytes recibidos: %d", b);
 	log_info(logger, "Operacion %d", atoi(buffer));

 	int operacion = atoi(buffer);
 	free(buffer);
 	char** desempaquetado=NULL;
 	char* paquete;
 	char* tamanioPaq = malloc(sizeof(char)*4);

 	if(operacion !=5 && operacion !=6 && operacion !=7){


 		recvData(cli,tamanioPaq, sizeof(char)*3);
 		tamanioPaq[3]='\0';
 		int tamanio = atoi(tamanioPaq);
 		log_info(logger, tamanioPaq);
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
 	char* tabla;
 	char* rtaFull;

 	switch (operacion) {
 				case 0: //SELECT
 	 					nombreTabla = desempaquetado[0];
 	 					key = atoi(desempaquetado[1]);
 	 					rta = mSelect(nombreTabla, key);
 	 					log_info(logger,rta);
 	 					//si no existe
 	 					if(strcmp(rta, "2")!=0){
 	 						if(strcmp(rta, "3")==0){
 	 							log_info(logger,"mando que no existe");
 	 							sendData(cli, rta, strlen(rta)+1);
 	 							free(rta);
 	 						}else{
 	 							char* msj;
 	 							msj = empaquetar(0, rta);
 	 							log_info(logger,"mando que existe rta:%s",msj);
 	 							sendData(cli, msj, strlen(msj)+1);
 	 							free(msj);
 	 							free(rta);
 	 						}
 	 					}
 	 					//si estoy full
 	 					else{
 	 						sendData(cli, rta, strlen(rta)+1);
 	 						free(rta);
 	 						rtaFull= malloc(2);
 	 						recvData(cli, rtaFull, sizeof(char));
 	 						rtaFull[1]='\0';
 	 					 	if(atoi(rtaFull)==5){
 	 					 		int r =mJournal();
 	 					 		if(r==0){
 	 					 			sendData(cli,"0",2);
 	 					 		}else{
 	 					 			sendData(cli,"1",2);
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

 					resp = mInsert(nombreTabla, key, value);
 					log_info(logger, "Antes de responder el insert");
 					rta = string_itoa(resp);
 					sendData(cli, rta, sizeof(char)*2);
 					log_info(logger, "Rta insert %d\n", resp);
 					free(rta);
 					rtaFull= malloc(2);

 					if(resp == 2){
 						recvData(cli, rtaFull, sizeof(char));
 						rtaFull[1]='\0';
 						if(atoi(rtaFull)==5){
 							mJournal();
 							resp = mInsert(nombreTabla, key, value);
 							rta = string_itoa(resp);
 							sendData(cli, rta, sizeof(char)*2);
 							log_info(logger, "Rta insert después de un journal %d\n", resp);
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
 					resp = mCreate(nombreTabla, consistencia, particiones, tiempoCompactacion);
 					rta = string_itoa(resp);
 					sendData(cli, rta, sizeof(char)*2);
 					free(rta);
 					break;

 				case 3: //DESCRIBE
 					usleep(config->retardoMem);

 					log_info(logger,"DESCribe");
 					log_info(logger,"holaa %s",desempaquetado[0]==NULL);
 					log_info(logger,"DESCribe");
 					nombreTabla = desempaquetado[0];
 					log_info(logger,desempaquetado[0]);
 					rta =mDescribe(nombreTabla);
 					sendData(cli, rta, strlen(rta)+1);
 					free(rta);
 					break;

 				case 4: //DROP
 					usleep(config->retardoMem);

 					nombreTabla = desempaquetado[0];
 					resp = mDrop(nombreTabla);
 					rta = string_itoa(resp);
 					sendData(cli, rta, sizeof(char)*2);
 					free(rta);

 					break;

 				case 5: //JOURNAL
 					resp = mJournal();
 					rta = string_itoa(resp);
 					sendData(cli, rta, sizeof(char)*2);
 					free(rta);
 					break;

 				case 6: //DEVOLVER TABLA DE ACTIVOS
 					usleep(config->retardoMem);
 					tabla = confirmarActivo();
 					sendData(cli, tabla, strlen(tabla)+1);
 					log_info(logger, "Tabla empaquetada %s", tabla);
 					free(tabla);
 				    break;

				case 7: //RECIBIR TABLA DE ACTIVOS DE LA MEMORIA QUE PIDIO CONFIRMACION

					rta=malloc(2);
					recvData(cli, rta, sizeof(char)*1);
					rta[1] = '\0';
				 	if(atoi(rta)==0){
				 		char*tamanio = malloc(sizeof(char)*4);
					 	recvData(cli,tamanio,sizeof(char)*3);
						tamanio[3] = '\0';
						log_info(logger, "Tamanio tabla en sting %s", tamanio);
						int tamtabla = atoi(tamanio);
						log_info(logger, "Tamanio tabla %d", tamtabla);
						char* bufferTabla = malloc(tamtabla+sizeof(char));
						recvData(cli,bufferTabla,tamtabla);
						desempaquetarTablaSecundaria(bufferTabla);
						cargarInfoDeSecundaria(0);
						free(rta);
						free(tamanio);
						free(bufferTabla);
				 	  }
				 	  else{
				 	    log_error(logger, "No pude recibir la tabla de activos de la otra memoria");
				 	    free(rta);
				 	   }
				 	   break;

 				}
 	//responder 0 si salio bien, 1 si salio mal
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


 void* consola(void* arg){

	int fin = (int*) arg;
	char* linea;
	while(1){
		linea = readline(">");

 		if(!strncmp(linea,"SELECT ",7))
 		{
 			char **subStrings= string_n_split(linea,3," ");
 			u_int16_t k=atoi(subStrings[2]);
 			mSelect(subStrings[1],k);
 			liberarSubstrings(subStrings);
 		}

 	 	if(!strncmp(linea,"INSERT ",7)){//INSERT "NOMBRE" 5/ "VALUE"
 	 		char **split= string_n_split(linea,4," ");
 	 		int key= atoi(split[2]);
 	 		char **cadena=string_split(split[3]," ");

 	 		mInsert(split[1],key,split[3]);


 	 		liberarSubstrings(cadena);
 	 		liberarSubstrings(split);
 	 	}

 	 	if(!strncmp(linea,"CREATE ",7)){
 			char **subStrings= string_n_split(linea,5," ");
 			u_int16_t particiones=atoi(subStrings[3]);
 			long timeCompaction=atol(subStrings[4]);
 			mCreate(subStrings[1],subStrings[2],particiones,timeCompaction);
 			log_info(logger,"Se hizo CREATE de la tabla: %s.",subStrings[1]);
 			liberarSubstrings(subStrings);
 		}

 		if(!strncmp(linea,"DESCRIBE",8)){
 			char **subStrings= string_n_split(linea,2," ");
 			mDescribe(subStrings[1]);
 			liberarSubstrings(subStrings);
 		}

 		if(!strncmp(linea,"DROP ",5)){
 			char **subStrings= string_n_split(linea,2," ");
 			if(subStrings[1]==NULL){
 				log_info(logger,"No se ingreso el nombre de la tabla.");
 			}
 			mDrop(subStrings[1]);
 			log_info(logger,"Se envio el drop a LFS y se borro de memoria la tabla %s");

 			free(subStrings[0]);
 			free(subStrings[1]);
 			free(subStrings);
 		}

 		if(!strncmp(linea,"JOURNAL",6)){
 			mJournal();
 		}

 		if(!strncmp(linea,"MOSTRAR",7)){
 			mostrarMemoria();
 		}
		
		if(!strncmp(linea,"FULL",4)){
			int resultado=0;
 			resultado=FULL();
			if(resultado)log_info(logger,"La memoria esta FULL");
			if(!resultado)log_info(logger,"La memoria NO esta FULL");
 		}
		if(!strncmp(linea,"GOSSIP",6)){
					mGossip();
		 		}

 		if(!strncmp(linea,"EXIT",5)){
 			free(linea);
 			fin = 0;
 			break;
 		}
 		free(linea);
 	}

 	return NULL;
 }

 void* journalProgramado(void *arg){


 	while(1){
 		usleep(config->retardoJournal);
 		log_info(logger,"Se realiza un journal programado");
 		mJournal();

 		}

 	return NULL;
 }

 void* gossipProgramado(void* arg){

 	while(1){
 		log_info(logger, "Se realiza un gossip programado");
 		mGossip();
 		usleep(config->retardoGossiping);
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



 u_int16_t handshakeConLissandra(u_int16_t lfsCliente,char* ipLissandra,u_int16_t puertoLissandra){
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
	 char* datos = formatearSelect(nombreTabla, key);
	 char* paqueteListo = empaquetar(0, datos);
	 char* value;
	 u_int16_t lfsSock = crearConexionLFS();

	 sendData(lfsSock, paqueteListo, strlen(paqueteListo)+1);
	 log_info(logger, "Paquete select %s", paqueteListo);
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

		 char* datos = formatearInsert(nombreTabla, timestamp, key, value);
		 char* paqueteListo = empaquetar(1, datos);
		 u_int16_t lfsSock = crearConexionLFS();
		 sendData(lfsSock, paqueteListo, strlen(paqueteListo)+1);
		 log_info(logger, "Paquete insert %s", paqueteListo);
		 free(datos);
		 free(paqueteListo);

		 char *buffer = malloc(sizeof(char)*2);
		 recvData(lfsSock, buffer, sizeof(char));
		 close(lfsSock);
		 buffer[1] = '\0';
		 int rta = atoi(buffer);
		 free(buffer);
		 return rta;


 }

 char* describeLissandra(char* nombreTabla){

	 u_int16_t lfsSock = crearConexionLFS();
	 char* describeGlobal = malloc(sizeof(char)*5);
	 strcpy(describeGlobal, "3000");

	 if(strcmp(nombreTabla, "global")!=0){
		 char* paqueteListo = empaquetar(3, nombreTabla);
		 sendData(lfsSock, paqueteListo, strlen(paqueteListo)+1);
		 log_info(logger, "Paquete describe con tabla %s", paqueteListo);
		 free(paqueteListo);

	 }
	 else{

		 sendData(lfsSock, describeGlobal, strlen(describeGlobal)+1);
		 log_info(logger, "Paquete describe %s", describeGlobal);

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

		 close(lfsSock);
		 log_info(logger, "Respuesta del describe %s", listaMetadatas);
		 free(buffer);
		 free(tam);

		 return listaMetadatas;
	 }
	 else{
		 return buffer;
	 }

 }

 int createLissandra(char* nombreTabla, char* criterio, u_int16_t nroParticiones, long tiempoCompactacion){
	 char* datos = formatearCreate(nombreTabla, criterio, nroParticiones, tiempoCompactacion);
	 char* paqueteListo = empaquetar(2, datos);
	 u_int16_t lfsSock = crearConexionLFS();
	 if(lfsSock == -1){
		 log_error(logger, "No se pudo conectar con LFS");
		 return 1;
	 }
	 sendData(lfsSock, paqueteListo, strlen(paqueteListo)+1);
	 log_info(logger, "Paquete create %s", paqueteListo);
	 free(datos);
	 free(paqueteListo);

	 char* buffer = malloc(sizeof(char)*2);
	 recvData(lfsSock, buffer, sizeof(char));
	 buffer[1] = '\0';
	 close(lfsSock);
	 int rta = atoi(buffer);
	 log_info(logger, "Respuesta del create %d", rta);
	 free(buffer);
	 return rta;
 }

 int dropLissandra(char* nombreTabla){
	 char* paqueteListo = empaquetar(4, nombreTabla);
	 u_int16_t lfsSock = crearConexionLFS();
	 if(lfsSock == -1){
	 	 log_error(logger, "No se pudo conectar con LFS");
	 	 return 1;
	  }
	 sendData(lfsSock, paqueteListo, strlen(paqueteListo)+1);
	 log_info(logger, "Paquete drop %s", paqueteListo);
	 free(paqueteListo);

	 char* buffer = malloc(sizeof(char)*2);
	 recvData(lfsSock, buffer, sizeof(char));
	 buffer[1] = '\0';
	 close(lfsSock);
	 int rta = atoi(buffer);
	 free(buffer);
	 return rta;

 }


 //---------------------------------------------------------//
 //------------------MANEJO DE MEMORIA---------------------//
 //-------------------------------------------------------//


 int memoriaLlena(){ //Devuelve 0 si esta llena (no se fija los flags modificados)
		log_info(logger,"entra a memoria llena ");
 	bool algunoLibre(marco* unMarco){
 		return unMarco->estaLibre == 0;
 	}
 	return list_any_satisfy(tablaMarcos,(void*)algunoLibre);
 }

 int todosModificados(segmento* aux){

	 int estaModificada(pagina* p){
		 return p->modificado == 1;
	 }
	 return list_all_satisfy(aux->tablaPaginas,(void*)estaModificada);
 }

 int FULL(){ //La memoria esta FULL si se cumple memoriaLlena() y todas las tablaPaginas estan con flag modificados


	 int total=0;
	 int tam =list_size(tablaSegmentos);
	 if(tam==0)return 0;
	 segmento* aux;

	 if(!memoriaLlena()){
		 for(int i = 0;i<tam;i++){
			 aux = list_get(tablaSegmentos,i);
			 total += todosModificados(aux);
		 }

	 }else{
		 return 0;
	 }
	 return total == tam;
 }

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


 int primerMarcoLibre(){
 	int posMarco = -1;
 	int i=0;
 	marco *unMarco;
 	pthread_mutex_lock(&lockTablaMarcos);
 	if(!FULL()){
 		if(memoriaLlena()){ //Si la memoria NO esta llena puede asignar un marco
 			log_info(logger,"entra a marcos libres");
 			while(i < cantMarcos){
 				unMarco = list_get(tablaMarcos,i);
				if((unMarco->estaLibre) == 0){
					pthread_mutex_lock(&unMarco->lockMarco);
					unMarco->estaLibre = 1;
					pthread_mutex_unlock(&unMarco->lockMarco);
					posMarco = unMarco->nroMarco;
					break;
				}
				else{
					i++;
				}
			}
		}
		else{
			log_info(logger,"entra a lru ");
			posMarco = LRU(); //te devuelve la posicion del marco a ocupar, liberando lo que habia antes
			unMarco = list_get(tablaMarcos,posMarco);
			pthread_mutex_lock(&unMarco->lockMarco);
			unMarco->estaLibre = 1;
			pthread_mutex_unlock(&unMarco->lockMarco);
		}
		pthread_mutex_unlock(&lockTablaMarcos);
		return posMarco;
 	}
 	else {
 		pthread_mutex_unlock(&lockTablaMarcos);
 		return -1;
 	}
 }


 void liberarMarco(int nroMarcoALiberar){
 	marco* nuevo = list_get(tablaMarcos,nroMarcoALiberar);
 	pthread_mutex_lock(&nuevo->lockMarco);
 	nuevo->estaLibre = 0;
 	pthread_mutex_unlock(&nuevo->lockMarco);

 }


 posMarcoUsado* crearPosMarcoUsado(int nroMarco,int pos){
	 posMarcoUsado* nuevo = malloc(sizeof(posMarcoUsado));
	 nuevo->nroMarco = nroMarco;
	 nuevo->posicionDeUso = pos;
	 return nuevo;
 }

 void agregarPosMarcoUsado(posMarcoUsado* nuevo){
	 list_add(listaDeUsos,nuevo);
 }

 int LRU(){
	 pthread_mutex_lock(&lockTablaUsos);
	 log_info(logger, "list size :%i", list_size(listaDeUsos));
	 int i=1,menor,tamLista,nroMarcoAborrar;
	 tamLista = list_size(listaDeUsos);
	 posMarcoUsado* aux= list_get(listaDeUsos,0);
	 menor = aux->posicionDeUso;
	 nroMarcoAborrar= aux->nroMarco;

	 while(i<tamLista){
		 aux = list_get(listaDeUsos,i);
		 if(menor > aux->posicionDeUso){
			 menor = aux->posicionDeUso;
			 nroMarcoAborrar = aux->nroMarco;
		 }else{
			 i++;
		 }
	 }
	 pthread_mutex_unlock(&lockTablaUsos);
	 eliminarDeListaUsos(nroMarcoAborrar);
	 liberarMarco(nroMarcoAborrar);
	 log_info(logger, "marco nro :%i",nroMarcoAborrar);
	 bool buscaMarco(pagina * pag){
		 return pag->nroMarco==nroMarcoAborrar;
	 }
	 void buscaSegYPag(segmento *s){
		 log_info(logger, "seg :%s", s->nombreTabla);

		 //list_remove_and_destroy_by_condition(s->tablaPaginas,(void*)buscaMarco,(void*)paginaDestroy);
		// pthread_mutex_lock(&s->lockSegmento);
		 list_remove_by_condition(s->tablaPaginas,(void*)buscaMarco);
		// pthread_mutex_unlock(&s->lockSegmento);//list_iterate(s->tablaPaginas,(void*)buscaMarco)
	 }
	 list_iterate(tablaSegmentos,(void*)buscaSegYPag);



	 void itera(marco *m){
		 log_info(logger,"marco %i libre: %i" , m->nroMarco , m->estaLibre);
	 }

	 list_iterate(tablaMarcos,(void*)itera);




	 log_info(logger, "marco nro :%i",nroMarcoAborrar);



		//Si llegamos aca la memoria SI o SI esta FULL 
		//Avisarle al Kernel FULL
	    //Devuelve 0 porque como la memoria queda vacia el 0 pasa a ser el primer marco vacio (DEBERIA)
	 liberarMarco(nroMarcoAborrar);

	 return nroMarcoAborrar;

	 //Esta funcion lee de una lista cual fue el marco que hace mas tiempo que no se usa
	 //lo libera y devuelve su posicion para que sea asignado a otra pagina

 }

 void agregarAListaUsos(int nroMarco){
	 pthread_mutex_lock(&lockTablaUsos);
 	posMarcoUsado* nuevo = crearPosMarcoUsado(nroMarco,posicionUltimoUso);

 	posicionUltimoUso++;

 	list_add(listaDeUsos,nuevo);
 	pthread_mutex_unlock(&lockTablaUsos);
 }


 void eliminarDeListaUsos(int nroMarcoAEliminar){
	 pthread_mutex_lock(&lockTablaUsos);
	 bool itera(posMarcoUsado *aux){
		 return aux->nroMarco == nroMarcoAEliminar;
	 }
	 list_remove_by_condition(listaDeUsos,(void*)itera);
	 pthread_mutex_unlock(&lockTablaUsos);
 }


 void actualizarListaDeUsos(int nroMarco){
	 pthread_mutex_lock(&lockTablaUsos);
	 log_info(logger,"holaa %i" , list_size(listaDeUsos));
	 /* 	void itera(posMarcoUsado *aux){
	  		log_info(logger,"marco %i", aux->nroMarco);
	  	}
	  	list_iterate(listaDeUsos,(void*) itera);*/

	 log_info(logger,"nro: %i" ,nroMarco);
 	int tieneMismoMarco(posMarcoUsado * aux){
 		log_info(logger,"true o false %i",aux->nroMarco == nroMarco);
 		log_info(logger,"Marco a actualizar %i",nroMarco);
 		log_info(logger,"Marco de la lista %i",aux->nroMarco);
 		return aux->nroMarco == nroMarco;
 	}
 	log_info(logger,"antes del listfind en lru");
 	posMarcoUsado* marcoParaActualizar = list_find(listaDeUsos,(void*) tieneMismoMarco);
 	log_info(logger,"despues del listfind en lru");
 	marcoParaActualizar->posicionDeUso = posicionUltimoUso;
 	posicionUltimoUso++;
 	pthread_mutex_unlock(&lockTablaUsos);
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

int conseguirIndexSeg(segmento* nuevo){

	int index=0;

	while(index < (tablaSegmentos->elements_count)){
		segmento* aux = list_get(tablaSegmentos,index);
		if(string_equals_ignore_case(aux->nombreTabla,nuevo->nombreTabla)){
			break;
		}
		else{
			index++;
		}
	}
	return index;
}



//---------------------------------------------------------//
//--------------------------BORRADO-----------------------//
//-------------------------------------------------------//


void eliminarSegmento(segmento* nuevo){

	/*void itera(pagina *p){
		if(p->modificado==)
	}*/
	//list_iterate(nuevo->tablaPaginas,(void*) itera);

	int index = conseguirIndexSeg(nuevo); //se usa para el free

	list_remove_and_destroy_element(tablaSegmentos,index,(void*)segmentoDestroy);


}

void paginaDestroy(pagina* pagParaDestruir){
	if((pagParaDestruir->modificado)==0){
	  	eliminarDeListaUsos(pagParaDestruir->nroMarco);
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
	free(config);

}

void finalizar(){
	list_destroy_and_destroy_elements(tablaSegmentos, (void*)segmentoDestroy);
	pthread_mutex_destroy(&lockTablaSeg);
	eliminarMarcos();
	pthread_mutex_destroy(&lockTablaMarcos);
	pthread_mutex_destroy(&lockConfig);

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

	log_info(logger, "Empieza el insert");
	sem_wait(&semJournal);
	log_info(logger, "Pasó el lock");
	log_info(logger,"\n\n LA KEY ES : %i " , key);
	if(strlen(valor)+1 <=maxValue){
		log_info(logger,"entra a insert ");

		segmento *seg = buscarSegmento(nombreTabla);
		pagina *pag;
		long timestampActual;

		if(seg != NULL){ //existe el segmento
			log_info(logger,"existe segmento ");
			pthread_mutex_lock(&seg->lockSegmento);
			pag = buscarPaginaConKey(seg, key);
				if (pag == NULL){ //no existe la pagina
					log_info(logger,"no existe pagina ");
					pag = crearPagina();
					if(pag->nroMarco==-1){
						pthread_mutex_unlock(&seg->lockSegmento);
						sem_post(&semJournal);
						log_info(logger, "Pasó el lock de salida");

						return 2;
					}
					agregarPagina(seg,pag);
					timestampActual = time(NULL);
					agregarDato(timestampActual, key, valor, pag);
					log_info(logger, "VOLVI DE AGREGAR DATO");
					pag->modificado = 1;
					log_info(logger,"hola" );
				//	log_info(logger,"\n\n\n\n tamanio de lista usos: %i \n\n\n\n\n\n", list_size(listaDeUsos));

				}else{ //existe la pagina
					log_info(logger,"existe pagina ");
					agregarDato(time(NULL),key,valor,pag);
					log_info(logger, "VOLVI DE AGREGAR DATO");
					//pag->modificado = 1;
					log_info(logger,"\n\n\n\n tamanio de lista usos: %i \n\n\n\n\n\n", list_size(listaDeUsos));


					if(pag->modificado!=1){
						eliminarDeListaUsos(pag->nroMarco);
					}
					pag->modificado = 1;
					log_info(logger,"\n\n\n\n tamanio de lista usos: %i \n\n\n\n\n\n", list_size(listaDeUsos));

				}
				pthread_mutex_unlock(&seg->lockSegmento);
		}else{ //no existe el segmento
			log_info(logger,"no existe segmento ");
			pagina *pag = crearPagina();
			if(pag->nroMarco==-1){
				sem_post(&semJournal);
				log_info(logger, "Pasó el lock de salida");

				return 2;
			}
			pthread_mutex_lock(&lockTablaSeg);
			seg = crearSegmento(nombreTabla);
			pthread_mutex_lock(&seg->lockSegmento);
			list_add(tablaSegmentos, seg);
			pthread_mutex_unlock(&lockTablaSeg);
			agregarPagina(seg,pag);
			timestampActual = time(NULL);
			agregarDato(timestampActual, key, valor, pag);
			log_info(logger, "VOLVI DE AGREGAR DATO");
			pag->modificado = 1;
			log_info(logger,"\n\n\n\n tamanio de lista usos: %i \n\n\n\n\n\n", list_size(listaDeUsos));

			pthread_mutex_unlock(&seg->lockSegmento);
		}
		log_info(logger, "Se inserto al segmento %s el valor %s", nombreTabla, valor);
		sem_post(&semJournal);
		log_info(logger, "Pasó el lock de salida");

		return 0;

	}
	/*else{
		log_info(logger,"La memoria esta FULL, no se puede hacer el INSERT");
		return 2;
	}
	}*/
	else{
		log_info(logger, "Se intentó insertar un value mayor al tamaño permitido");
		sem_post(&semJournal);
		log_info(logger, "Pasó el lock de salida");
		return 1;
	}

}



char* mSelect(char* nombreTabla,u_int16_t key){

	log_info(logger, "Empieza el select");
	sem_wait(&semJournal);
	log_info(logger, "Pasó el lock");

	segmento *nuevo = buscarSegmento(nombreTabla);
	pagina* pNueva;
	char* valorPagNueva;
	char* valor;
	char* noExiste =string_duplicate("3");
	char* estoyFull = string_duplicate("2");

	log_info(logger, "Se pidio un select de la tabla %s key %d", nombreTabla, key);
	if(nuevo!= NULL){

		pNueva = buscarPaginaConKey(nuevo,key);

		if(pNueva != NULL){
			valor = (char*)conseguirValor(pNueva);
			log_info(logger, "Se seleccionó el valor %s", valor);
			log_info(logger, "Encontro segmento y pagina");

			if(pNueva->modificado == 0){
				log_info(logger,"\n\n\n\n tamanio de lista usos: %i \n\n\n\n\n\n", list_size(listaDeUsos));

				actualizarListaDeUsos(pNueva->nroMarco);
				log_info(logger,"\n\n\n\n tamanio de lista usos: %i \n\n\n\n\n\n", list_size(listaDeUsos));

			}
			log_info(logger, "salio lru");
			log_info(logger, "Se seleccionó el valor despues de lru %s", valor);

			log_info(logger,"\n\n\n\n tamanio de lista usos: %i \n\n\n\n\n\n", list_size(listaDeUsos));
			sem_post(&semJournal);
			log_info(logger, "Pasó el lock de salida");
			free(estoyFull);
			free(noExiste);
			return valor;
		}
		else{
			//if(!FULL()){
			pNueva = crearPagina();
			if(pNueva->nroMarco==-1){
				sem_post(&semJournal);
				log_info(logger, "Pasó el lock de salida");

				free(noExiste);
				return estoyFull;
			}
			valorPagNueva = selectLissandra(nombreTabla,key);
			if(valorPagNueva != NULL){
				pNueva->modificado = 0;
				pthread_mutex_lock(&nuevo->lockSegmento);
				agregarPagina(nuevo,pNueva);
				pthread_mutex_unlock(&nuevo->lockSegmento);
				agregarDato(time(NULL),key,valorPagNueva,pNueva);
				log_info(logger,"\n\n\n\n tamanio de lista usos: %i \n\n\n\n\n\n", list_size(listaDeUsos));
				agregarAListaUsos(pNueva->nroMarco);
				log_info(logger,"\n\n\n\n tamanio de lista usos: %i \n\n\n\n\n\n", list_size(listaDeUsos));

				log_info(logger, "Se seleccionó el valor %s", valorPagNueva);
				sem_post(&semJournal);
				log_info(logger, "Pasó el lock de salida");
				free(estoyFull);
				free(noExiste);
				return valorPagNueva;
			}
				else{
					sem_post(&semJournal);
					log_info(logger, "Pasó el lock de salida");
					free(estoyFull);
					return noExiste;
				}

		}
	}
	else{
		//if(!FULL()){
		pNueva = crearPagina();
		if(pNueva->nroMarco==-1){
			sem_post(&semJournal);
			log_info(logger, "Pasó el lock de salida");
			free(noExiste);
			return estoyFull;
		}
		pthread_mutex_lock(&lockTablaSeg);
		nuevo = crearSegmento(nombreTabla);
		list_add(tablaSegmentos, nuevo);
		pthread_mutex_unlock(&lockTablaSeg);
		valorPagNueva = selectLissandra(nombreTabla,key);
		if(valorPagNueva !=NULL){
			pNueva->modificado = 0;
			pthread_mutex_lock(&nuevo->lockSegmento);
			agregarPagina(nuevo,pNueva);
			pthread_mutex_unlock(&nuevo->lockSegmento);

			agregarDato(time(NULL),key,valorPagNueva,pNueva);
			log_info(logger,"\n\n\n\n tamanio de lista usos: %i \n\n\n\n\n\n", list_size(listaDeUsos));

			agregarAListaUsos(pNueva->nroMarco);
			log_info(logger,"\n\n\n\n tamanio de lista usos: %i \n\n\n\n\n\n", list_size(listaDeUsos));


			log_info(logger, "Se seleccionó el valor %s", valorPagNueva);
			sem_post(&semJournal);
			log_info(logger, "Pasó el lock de salida");
			free(estoyFull);
			free(noExiste);
			return valorPagNueva;

		}
		else{
			sem_post(&semJournal);
			log_info(logger, "Pasó el lock de salida");
			free(estoyFull);
			return noExiste;
		}
		//}
	/*	else{
			return estoyFull;
		}*/
	}


}

int mCreate(char* nombreTabla, char* criterio, u_int16_t nroParticiones, long tiempoCompactacion){

	int rta = createLissandra(nombreTabla,criterio,nroParticiones,tiempoCompactacion);
	log_info(logger, "La respuesta del create fue %d", rta);

	return rta;
}


char* mDescribe(char* nombreTabla){

	char* rta = describeLissandra(nombreTabla);
	if(atoi(rta)!=0){
		return rta;
	}
	else{
		char* respuesta = string_from_format("%s%s", "0", rta);
		free(rta);
		return respuesta;

	}


}

int mDrop(char* nombreTabla){

	sem_wait(&semJournal);

	segmento* nuevo = buscarSegmento(nombreTabla);

	if(nuevo != NULL){
		pthread_mutex_lock(&lockTablaSeg);
		eliminarSegmento(nuevo);
		pthread_mutex_unlock(&lockTablaSeg);
		log_info(logger, "Se realizo un drop del segmento %s", nombreTabla);

	}
	sem_post(&semJournal);
	int rta = dropLissandra(nombreTabla);

	return rta;
}


int mJournal(){

	for(int i = 0; i<config->multiprocesamiento; i++){
		sem_wait(&semJournal);
	}

	log_info(logger, "\n\n Inicio del journal, se bloquea la tabla de segmentos \n\n\n\n\n\n\n\n\n\n\n");
	pthread_mutex_lock(&lockTablaSeg);
	pthread_mutex_lock(&lockTablaMarcos);


	for(int i = 0; i<tablaSegmentos->elements_count; i++){
		segmento *seg = list_get(tablaSegmentos, i);
		pthread_mutex_lock(&seg->lockSegmento);
	}


	for(int i =0; i<(tablaSegmentos->elements_count); i++){
		char* nombreSegmento = malloc(sizeof(char)*100);
		segmento *seg = list_get(tablaSegmentos, i);
		strcpy(nombreSegmento, seg->nombreTabla);
		t_list *paginasMod;
		if(list_is_empty(seg->tablaPaginas)){
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
		}


	}

	log_info(logger, "Fin del journal, procede a borrar datos existentes");
	list_clean_and_destroy_elements(tablaSegmentos, (void*)segmentoDestroy);
	int listaVacia = list_is_empty(tablaSegmentos);
	if (listaVacia == 1){
		marco *marc;
		for(int i =0; i<tablaMarcos->elements_count; i++){
			marc = list_get(tablaMarcos, i);
			log_info(logger, "Marco %i, libre: %i", marc->nroMarco, marc->estaLibre);
		}
		log_info(logger, "Datos borrados, se desbloquea la tabla de segmentos");

		log_info(logger,"size usos %d" ,  list_size(listaDeUsos));
		pthread_mutex_unlock(&lockTablaSeg);
		pthread_mutex_unlock(&lockTablaMarcos);
		void itera(marco *marco){
			marco->estaLibre=0;
		}
		list_iterate(tablaMarcos,(void*)itera);

		for(int i = 0; i<config->multiprocesamiento; i++){
			sem_post(&semJournal);

		}

		return 0;
	}
	else{
		log_error(logger, "Algo salió mal al vaciar la lista de segmentos");
		return 1;
	}

}

//NROMEM;IP;PUERTO SUPER SEND CON TABLA ENTERA

char* formatearTablaGossip(int nro,char*ip,char*puerto){
	char* id = string_itoa(nro);
	char* paquetin = string_from_format("%s;%s;%s;",id,ip,puerto);
	log_info(logger, "ID EMPAQUETADO %s", id);
	free(id);
	return paquetin;
}


char* empaquetarTablaActivas(){
	int i=0;
	char* paquetote=string_new();
	infoMemActiva* aux = list_get(tablaMemActivas,i);

	while(i<(tablaMemActivas->elements_count)){
		if(aux->activa){
		string_append(&paquetote,formatearTablaGossip(aux->nroMem,aux->ip,aux->puerto));
		}
		i++;
		aux = list_get(tablaMemActivas,i);
	}
	return paquetote;
}
void desempaquetarTablaSecundaria(char* paquete){
	pthread_mutex_lock(&lockTablaMemSec);
	int i=0;
	char** split = string_split(paquete,";");

	while(split[i]){
		infoMemActiva* aux = malloc(sizeof(infoMemActiva));
		aux->nroMem = atoi(split[i]);
		log_info(logger, "ID memoria %s y en num %d", split[i], aux->nroMem);
		i++;
		aux->ip = split[i];
		i++;
		aux->puerto = split[i];
		i++;
		list_add(tablaMemActivasSecundaria,aux);
	}
	pthread_mutex_unlock(&lockTablaMemSec);
} // desempaqueta la tabla recibida y la carga en la lista secundaria que es global

int pedirConfirmacion(char*ip,char* puerto){
	u_int16_t cliente;
	char* codOpe = "6";
	u_int16_t puertoAux=atoi(puerto);
	int conexion = linkClient(&cliente,ip,puertoAux,0);

	if(conexion ==1){
		return 0; // no esta activa la memoria
	}

	char* rta=malloc(sizeof(char)*2);
	sendData(cliente,codOpe,sizeof(char)*2); //Le mando el codigo para que me mande su tabla
	recvData(cliente,rta,sizeof(char));
	rta[1] = '\0';
	char* tamTabla=malloc(sizeof(char)*4);
	recvData(cliente,tamTabla,sizeof(char)*3);
	tamTabla[3] = '\0';
	char* buffer=malloc(atoi(tamTabla)+1);
	recvData(cliente,buffer,atoi(tamTabla));
	log_info(logger, "PAQUETE RECIBIDO %s", buffer);
	desempaquetarTablaSecundaria(buffer);  //aca actualizo mi tabla con lo que me envian
	close(cliente);

	//Le envio mi tabla
	u_int16_t nuevoCli;
	conexion = linkClient(&nuevoCli,ip,puertoAux,0);
	if(conexion ==1){
			return 0; // no esta activa la memoria
	}
	codOpe = "7";
	char*paquete = confirmarActivo();
	char*paqueteEntero = malloc(strlen(paquete)+2);
	strcpy(paqueteEntero,codOpe);
	string_append(&paqueteEntero,paquete);
	log_info(logger, "Paquete entero %s", paqueteEntero);
	sendData(cliente,paqueteEntero,strlen(paqueteEntero)+1);

	return 1;
} // devuelve si confirmo con 1 y recibe la tablaSecundaria y envio mi tabla

char* confirmarActivo(){ // podria recibir la ip y puerto del que pidio la confirmacion
   sem_wait(&lockTablaMemAct);
   char* paquete=empaquetarTablaActivas();
   char* paqueteListo=empaquetar(0,paquete);
   sem_post(&lockTablaMemAct);
   return paqueteListo;
} //un listen y da el ok a otra mem al enviarle su tablaMemActivas //tendria que haber un hilo siempre escuchando

int estaRepetido(char*ip){
	int mismaIp(infoMemActiva* aux){
		return (strcmp(aux->ip,ip)==0);
	}
	return list_any_satisfy(tablaMemActivas,(void*)mismaIp); //Devuelve 1 si hay repetido
}

void cargarInfoDeSecundaria(int i){ // el i decide si se cargo la primera antes o no
	pthread_mutex_lock(&lockTablaMemSec);
	int tam = list_size(tablaMemActivasSecundaria);
	if(tam != 0){
		infoMemActiva*aux=list_get(tablaMemActivasSecundaria,i);
		while(aux){
			if(!estaRepetido(aux->ip)){
				aux->activa=1;
				list_add(tablaMemActivas,aux);//si NO esta repetido lo agrega y sino lo pasa de largo
				}
			i++;
			aux=list_get(tablaMemActivasSecundaria,i);
		}
	}
	pthread_mutex_unlock(&lockTablaMemSec);
}

void agregarMemActiva(int id,char* ip,char*puerto){ //Se agrega a la lista //falta usar la secundaria y agregarle su info a la ppal y la config
	sem_wait(&lockTablaMemAct);
	if(!estaRepetido(ip)){
	infoMemActiva* nueva = malloc(sizeof(infoMemActiva));
	nueva->ip=malloc(strlen(ip)+1);
	nueva->puerto=malloc(strlen(puerto)+1);
	memcpy(nueva->ip,ip,strlen(ip)+1);
	memcpy(nueva->puerto,puerto,strlen(puerto)+1);
	nueva->nroMem=id;
	nueva->activa=1;
	list_add(tablaMemActivas,nueva);
	}else{
		int i = 1;
		infoMemActiva* aux = list_get(tablaMemActivas,i);
		while(aux->nroMem != id){
			i++;
			aux = list_get(tablaMemActivas,i);
		}
		aux->activa=1;
	}
	cargarInfoDeSecundaria(1);
	sem_post(&lockTablaMemAct);
}

int conseguirIdSecundaria(){ //Devuelve el id de la memoria que confirmo que esta activa
	infoMemActiva* aux = list_get(tablaMemActivasSecundaria,0); //es la posicion cero porque como confirmo hay una nueva tabla secundaria
	return aux->nroMem;
}

void estaEnActivaElim(char*ip){ //Si estaba en la tablaMemActivas la elimina
	sem_wait(&lockTablaMemAct);
    int i=1;
    infoMemActiva* aux = list_get(tablaMemActivas,i);
    while(aux){
        if(strcmp(aux->ip,ip)==0){
            aux->activa=0;
        }
        i++;
        aux = list_get(tablaMemActivas,i);
    }
    sem_post(&lockTablaMemAct);
}

void mostrarActivas(){
	int i = 0;
	infoMemActiva* aux = list_get(tablaMemActivas,i);
	printf("\nTabla Activas: \n");
	while(aux){
		printf("ID: %i \n",aux->nroMem);
		printf("IP: %s \n",aux->ip);
		printf("Activa : %i \n",aux->activa);
		printf("---------- \n");
		i++;
		aux = list_get(tablaMemActivas,i);
	}
}

void mGossip(){

    int i=0;
    pthread_mutex_lock(&lockConfig);
    t_config *configGossiping = read_config();
    char* ipsConfig = config_get_string_value(configGossiping,"IP_SEEDS");
    char* seedsConfig = config_get_string_value(configGossiping,"PUERTO_SEEDS");
    ipSeeds = string_split(ipsConfig,";");
    puertoSeeds = string_split(seedsConfig,";");

    while(ipSeeds[i]){
    	if(pedirConfirmacion(ipSeeds[i],puertoSeeds[i])){
    		int id = conseguirIdSecundaria();
		log_info(logger,"Se confirmo la memoria con id: %i",id);
    		agregarMemActiva(id,ipSeeds[i],puertoSeeds[i]);
    	}else{
    		estaEnActivaElim(ipSeeds[i]);
		log_info(logger,"La memoria con ip : %s no esta activa",ipSeeds[i]);
    	}
    	i++;
    }
 /*   i=1;
    infoMemActiva* aux = list_get(tablaMemActivas,i);
    while(aux){
    	if(pedirConfirmacion(aux->ip,aux->puerto)){
    		int id = conseguirIdSecundaria();
    		log_info(logger,"Se confirmo la memoria con id: %i",id);
       		agregarMemActiva(id,ipSeeds[i],puertoSeeds[i]);
 	    }else{
    	    estaEnActivaElim(ipSeeds[i]);
    		log_info(logger,"La memoria con ip : %s no esta activa",ipSeeds[i]);
    	}
    	i++;
    	aux=list_get(tablaMemActivas,i);
    } */
    log_info(logger,"Termino el gossip");
    mostrarActivas();
 	config_destroy(configGossiping);
    pthread_mutex_unlock(&lockConfig);
}

