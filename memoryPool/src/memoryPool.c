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

/*	mInsert("POSTRES", 1, "FLAN");
	mInsert("POSTRES", 2, "HELADO");
	mInsert("POSTRES", 3, "CHOCOLATE");
	mSelect("PROFESIONES", 1);
	mostrarMemoria();
	sleep(5);
	mInsert("PROFESIONES", 2, "MAESTRO");
	mInsert("PROFESIONES", 5, "CHEF");
	mInsert("ANIMALES", 1, "GATO");
	mInsert("ANIMALES", 2, "PERRO");
	mInsert("ANIMALES", 3, "JIRAFA");
*/
		pthread_t inotify;
	pthread_create(&inotify, NULL, correrInotify, NULL);



	//pthread_t gossipTemporal;
	//pthread_create(&gossipTemporal, NULL, gossipProgramado, NULL);
	int *fin;
	pthread_t hiloConsola;
	pthread_create(&hiloConsola, NULL, consola, NULL);

	pthread_t gestorConexiones;
	pthread_create(&gestorConexiones, NULL, gestionarConexiones, NULL);


	//pthread_t journalTemporal;
	//pthread_create(&journalTemporal, NULL, journalProgramado, NULL);


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

	mostrarMemoria();
	finalizar();
	return EXIT_SUCCESS;
}

//---------------------------------------------------------//
//------------------AUXILIARES DE ARRANQUE----------------//
//-------------------------------------------------------//

 int inicializar(){
	logger = init_logger();
	configuracion = read_config();
	int tamanioMemoria = config_get_int_value(configuracion, "TAM_MEM");
	u_int16_t puertoFS = config_get_int_value(configuracion,"PUERTO_FS");
	retardoJournal = config_get_int_value(configuracion,"RETARDO_JOURNAL")*1000;
	retardoGossip = config_get_int_value(configuracion, "RETARDO_GOSSIPING")*10000;
	pathConfig = malloc(strlen("/home/utnso/tp-2019-1c-donSaturados/memoryPool/memoryPool.config")+1);
	strcpy(pathConfig, "/home/utnso/tp-2019-1c-donSaturados/memoryPool/memoryPool.config");
	abortar = 1;

	char* ipFS = malloc(15);
	strcpy(ipFS, config_get_string_value(configuracion,"IP_FS"));
	posicionUltimoUso = 0; //Para arrancar la lista de usos en 0, ira aumentando cuando se llene NO TOCAR PLS
	prepararGossiping();

	log_info(logger, "Se inicializo la memoria con tamanio %d", tamanioMemoria);
	memoria = calloc(1,tamanioMemoria);

	if(memoria == NULL){
		log_error(logger, "Falló el inicio de la memoria principal, abortando ejecución");
		return 1;
	}


	u_int16_t lfsServidor;
	maxValue = handshakeConLissandra(lfsServidor, ipFS, puertoFS);

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

	pthread_mutex_init(&lockTablaSeg, NULL);
	pthread_mutex_init(&lockTablaMarcos, NULL);


	return 0;
}

 void prepararGossiping(){ //Hace las configuraciones iniciales del gossiping, NO lo empieza solo lo deja configurado
	 
	 char* ipsConfig = config_get_string_value(configuracion,"IP_SEEDS");
	 char* seedsConfig = config_get_string_value(configuracion,"PUERTO_SEEDS");
	 
	 idMemoria = config_get_int_value(configuracion,"MEMORY_NUMBER"); //Unico de cada proceso
	 tablaMemActivas = list_create();
	 tablaMemActivasSecundaria = list_create();
	 ipSeeds = string_split(ipsConfig,";");
	 puertoSeeds = string_split(seedsConfig,";");
	 infoMemActiva* nueva = malloc(sizeof(infoMemActiva));
	 nueva->nroMem = idMemoria;
	 nueva->ip = ipSeeds[0];
	 nueva->puerto = puertoSeeds[0];
	 list_add(tablaMemActivas,nueva);
	 //agregarMemActiva(idMemoria,ipSeeds[0],puertoSeeds[0]); lo dejo porque quizas te dan la config vacia
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
 	char** desempaquetado;
 	char* paquete;
 	char* tamanioPaq = malloc(sizeof(char)*4);

 	if(operacion !=5 && operacion !=6){


 		recvData(cli,tamanioPaq, sizeof(char)*3);
 		tamanioPaq[3]='\0';
 		int tamanio = atoi(tamanioPaq);
 		log_info(logger, tamanioPaq );

 		if(tamanio!=0){
 			paquete = malloc(tamanio + sizeof(char));
 			recvData(cli, paquete, tamanio);

 			desempaquetado = string_n_split(paquete, 5, ";");

 		}
 		else{
 			log_info(logger, "Antes strcpy");
 			strcpy(desempaquetado[0], "global");
 			log_info(logger, "Despues strcpy");
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
 						//char* msj = malloc(strlen(rta)+4);
 						if(strcmp(rta, "3")==0){
 							log_info(logger,"mando que no existe");
 							sendData(cli, rta, strlen(rta)+1);
 						}else{

 							char* msj = malloc(strlen(rta)+4);
 							msj = empaquetar(0, rta);
 							log_info(logger,"mando que existe rta:%s",msj);
 							sendData(cli, msj, strlen(msj)+1);
 						}
 					}
 					//si estoy full
 					else{
 						rtaFull= malloc(2);

 							sendData(cli, rta, strlen(rta)+1);
 							recvData(cli, rtaFull, sizeof(char));
 						 	rtaFull[1]='\0';
 						 	if(atoi(rtaFull)==5){
 						 		mJournal();
 						 		rta = mSelect(nombreTabla, key);
 						 		char* msj = malloc(strlen(rta)+4);
 						 		msj = empaquetar(0, rta);
 						 		sendData(cli, msj, strlen(msj)+1);
 						 		log_info(logger, "Rta select después de un journal %s\n", msj);
 						 		}

 					}
 					//si no existe en fs
 					break;

 				case 1: //INSERT
 					nombreTabla = desempaquetado[0];
 					key = atoi(desempaquetado[1]);
 					value = desempaquetado[2];

 					resp = mInsert(nombreTabla, key, value);
 					log_info(logger, "Antes de responder el insert");
 					sendData(cli, string_itoa(resp), sizeof(char)*2);
 					log_info(logger, "Rta insert %d\n", resp);
 					rtaFull= malloc(2);

 					if(resp == 2){
 						recvData(cli, rtaFull, sizeof(char));
 						buffer[1]='\0';
 						if(atoi(rtaFull)==5){
 							mJournal();
 							resp = mInsert(nombreTabla, key, value);
 							sendData(cli, string_itoa(resp), sizeof(char)*2);
 							log_info(logger, "Rta insert después de un journal %d\n", resp);
 							}
 						}

 					break;

 				case 2: //CREATE
 					nombreTabla = desempaquetado[0];
 					consistencia = desempaquetado[1];
 					particiones = atoi(desempaquetado[2]);
 					tiempoCompactacion = atol(desempaquetado[3]);
 					resp = mCreate(nombreTabla, consistencia, particiones, tiempoCompactacion);
 					sendData(cli, string_itoa(resp), sizeof(char)*2);
 					break;

 				case 3: //DESCRIBE
 					nombreTabla = desempaquetado[0];
 					log_info(logger,desempaquetado[0]);
 					rta =mDescribe(nombreTabla);
 					sendData(cli, rta, strlen(rta)+1);
 					break;

 				case 4: //DROP
 					nombreTabla = desempaquetado[0];
 					resp = mDrop(nombreTabla);
 					sendData(cli, string_itoa(resp), sizeof(char)*2);

 					break;

 				case 5: //JOURNAL
 					resp = mJournal();
 					sendData(cli, string_itoa(resp), sizeof(char)*2);
 					break;

 				case 6: //DEVOLVER TABLA DE ACTIVOS
 					tabla =confirmarActivo();
 					sendData(cli, tabla, strlen(tabla)+1);
 					char*tamanio = malloc(sizeof(4));
 					recvData(cli,tamanio,3);
 					char*bufferTabla=malloc(atoi(tamanio)+1);
 					recvData(cli,bufferTabla,atoi(tamanio));
 					desempaquetarTablaSecundaria(bufferTabla);
 					break;


 				}
 	//responder 0 si salio bien, 1 si salio mal
 	close(cli);
 	return NULL;
 }

 void* gestionarConexiones (void* arg){
 	u_int16_t puertoServer = config_get_int_value(configuracion, "PUERTO");
 	char* ipServer = config_get_string_value(configuracion, "IP");
 	u_int16_t server;


 	int servidorCreado = createServer(ipServer, puertoServer, &server);

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

 	while(abortar){
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
 	configInotify = read_config();
 	retardoJournal = config_get_int_value(configInotify,"RETARDO_JOURNAL")*1000;
 	retardoGossip = config_get_int_value(configInotify, "RETARDO_GOSSIPING")*10000;
 	log_info(logger, "Se modificó la config");
 	config_destroy(configInotify);

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
 		usleep(retardoJournal);
 		log_info(logger,"Se realiza un journal programado");
 		mJournal();

 		}

 	return NULL;
 }

 void* gossipProgramado(void* arg){

 	while(1){
 		log_info(logger, "Se realiza un gossip programado");
 		mGossip();
 		usleep(retardoGossip);
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
 	 return msj;

 }

 char* formatearSelect(char* nombreTabla, u_int16_t key){
	char* paquete = string_from_format("%s;%s", nombreTabla, string_itoa(key));
	return paquete;
}
 char* formatearInsert(char* nombreTabla, long timestamp, u_int16_t key, char* value){
	char* paquete = string_from_format("%s;%s;%s;%s", nombreTabla, string_itoa(key), value, string_itoa(timestamp));
	return paquete;
}

 char* formatearCreate(char* nombreTabla, char* consistencia, int particiones, long tiempoCompactacion){
	char* paquete= string_from_format("%s;%s;%s;%s", nombreTabla, consistencia, string_itoa(particiones), string_itoa(tiempoCompactacion));
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
 	u_int16_t maxV = atoi(buffer);
 	return maxV;
 }


 int crearConexionLFS(){
 	//crea un socket para comunicarse con lfs, devuelve el file descriptor conectado
 	int puertoFS = config_get_int_value(configuracion,"PUERTO_FS");
 	char* ipFS = config_get_string_value(configuracion,"IP_FS");

 	u_int16_t lfsServer;
 	int rta = linkClient(&lfsServer, ipFS, puertoFS, 1);

 	while(rta!=0){
 		log_error(logger, "No se pudo crear una conexión con LFS, volverá a intentarse");
 	 	int rta = linkClient(&lfsServer, ipFS, puertoFS, 1);

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

	 char* buffer = malloc(sizeof(char)*2);
	 recvData(lfsSock, buffer, sizeof(char));
	 if(atoi(buffer)==0){
		 char* tam = malloc(sizeof(char)*4);

		 recvData(lfsSock, tam, sizeof(char)*3);
		 tam[4]= '\0';
		 int t = atoi(tam);

		 log_info(logger, "Tamanio value %d", t);
		 value = malloc(t+sizeof(char));
		 recvData(lfsSock, value, t);
		 log_info(logger, "Se recibio el valor %s", value);

	 }
	 else{
		 value = NULL;
		 log_info(logger, "No existe un valor con esa key en LFS");
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

		 char *buffer = malloc(sizeof(char)*2);
		 recvData(lfsSock, buffer, sizeof(char));
		 close(lfsSock);
		 int rta = atoi(buffer);
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

	 }
	 else{

		 sendData(lfsSock, describeGlobal, strlen(describeGlobal)+1);
		 log_info(logger, "Paquete describe %s", describeGlobal);

	 }


	 //recibir
	 char* buffer = malloc(sizeof(char)*2);
	 recvData(lfsSock, buffer, sizeof(char));
	 char* tam = malloc(sizeof(char)*5);
	 recvData(lfsSock, tam, sizeof(char)*4);
	 int tamanio = atoi(tam);
	 char* listaMetadatas = malloc(tamanio+1);
	 recvData(lfsSock, listaMetadatas, tamanio);

	 close(lfsSock);
	 log_info(logger, "Respuesta del describe %s", listaMetadatas);

	 return listaMetadatas;
 }

 int createLissandra(char* nombreTabla, char* criterio, u_int16_t nroParticiones, long tiempoCompactacion){
	 char* datos = formatearCreate(nombreTabla, criterio, nroParticiones, tiempoCompactacion);
	 char* paqueteListo = empaquetar(2, datos);
	 u_int16_t lfsSock = crearConexionLFS();
	 if(lfsSock == -1){
		 log_error(logger, "No se pudo conectar con LFS");
	 }
	 sendData(lfsSock, paqueteListo, strlen(paqueteListo)+1);
	 log_info(logger, "Paquete create %s", paqueteListo);
	 char* buffer = malloc(sizeof(char)*2);
	 recvData(lfsSock, buffer, sizeof(char));

	 close(lfsSock);
	 int rta = atoi(buffer);
	 log_info(logger, "Respuesta del create %d", rta);
	 return rta;
 }

 int dropLissandra(char* nombreTabla){
	 char* paqueteListo = empaquetar(4, nombreTabla);
	 u_int16_t lfsSock = crearConexionLFS();
	 if(lfsSock == -1){
	 	 log_error(logger, "No se pudo conectar con LFS");
	  }
	 log_info(logger, "Paquete drop %s", paqueteListo);

	 sendData(lfsSock, paqueteListo, strlen(paqueteListo)+1);
	 char* buffer = malloc(sizeof(char)*2);
	 recvData(lfsSock, buffer, sizeof(char));

	 close(lfsSock);
	 int rta = atoi(buffer);
	 return rta;

 }


 //---------------------------------------------------------//
 //------------------MANEJO DE MEMORIA---------------------//
 //-------------------------------------------------------//


 int memoriaLlena(){ //Devuelve 0 si esta llena (no se fija los flags modificados)

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
 	if(memoriaLlena()){ //Si la memoria NO esta llena puede asignar un marco

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
 		posMarco = LRU(); //te devuelve la posicion del marco a ocupar, liberando lo que habia antes
		unMarco = list_get(tablaMarcos,posMarco);
		pthread_mutex_lock(&unMarco->lockMarco);
	 	unMarco->estaLibre = 1;
	 	pthread_mutex_unlock(&unMarco->lockMarco);
 	}
 	pthread_mutex_unlock(&lockTablaMarcos);
 	return posMarco;
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
	 eliminarDeListaUsos(nroMarcoAborrar);
		//Si llegamos aca la memoria SI o SI esta FULL 
		//Avisarle al Kernel FULL
	    //Devuelve 0 porque como la memoria queda vacia el 0 pasa a ser el primer marco vacio (DEBERIA)
	 liberarMarco(nroMarcoAborrar);

	 return nroMarcoAborrar;

	 //Esta funcion lee de una lista cual fue el marco que hace mas tiempo que no se usa
	 //lo libera y devuelve su posicion para que sea asignado a otra pagina

 }

 void agregarAListaUsos(int nroMarco){
 	posMarcoUsado* nuevo = crearPosMarcoUsado(nroMarco,posicionUltimoUso);
 	posicionUltimoUso++;
 	list_add(listaDeUsos,nuevo);
 }


 void eliminarDeListaUsos(int nroMarcoAEliminar){
	 bool itera(posMarcoUsado *aux){
		 return aux->nroMarco == nroMarcoAEliminar;
	 }
	 list_remove_by_condition(listaDeUsos,(void*)itera);
 }


 void actualizarListaDeUsos(int nroMarco){
	 log_info(logger,"holaa %i" , list_size(listaDeUsos));
	  	void itera(posMarcoUsado *aux){
	  		log_info(logger,"marco %i", aux->nroMarco);
	  	}
	  	list_iterate(listaDeUsos,(void*) itera);

	 log_info(logger,"nro: %i" ,nroMarco);
 	int tieneMismoMarco(posMarcoUsado * aux){
 		log_info(logger,"true o false %i",aux->nroMarco != nroMarco);
 		return aux->nroMarco != nroMarco;
 	}
 	log_info(logger,"antes del listfind en lru");
 	posMarcoUsado* marcoParaActualizar = list_find(listaDeUsos,(void*) tieneMismoMarco);
 	log_info(logger,"despues del listfind en lru");
 	marcoParaActualizar->posicionDeUso = posicionUltimoUso;
 	posicionUltimoUso++;
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
	char* a = malloc(maxValue);
	memcpy(value, ((memoria) + sizeof(long) + sizeof(u_int16_t)+ (pNueva->nroMarco)*offsetMarco),maxValue);
	memcpy(a, ((memoria) + sizeof(long) + sizeof(u_int16_t)+ (pNueva->nroMarco)*offsetMarco),maxValue);
	log_info(logger,"valorcito en conseguir valor: %s",a);

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

void finalizar(){
	list_destroy_and_destroy_elements(tablaSegmentos, (void*)segmentoDestroy);
	pthread_mutex_destroy(&lockTablaSeg);
	eliminarMarcos();
	pthread_mutex_destroy(&lockTablaMarcos);


	free(memoria);
	config_destroy(configuracion);
	log_info(logger, "Memoria limpia, adiós mundo cruel");
	log_destroy(logger);

}




//-------------------------------------//
//---------------API------------------//
//-----------------------------------//



int mInsert(char* nombreTabla, u_int16_t key, char* valor){
	log_info(logger,"\n\n LA KEY ES : %i " , key);
	if(strlen(valor)+1 <=maxValue){
	if(!FULL()){
		segmento *seg = buscarSegmento(nombreTabla);
		pagina *pag;
		long timestampActual;

		if(seg != NULL){ //existe el segmento
			pthread_mutex_lock(&seg->lockSegmento);
			pag = buscarPaginaConKey(seg, key);
				if (pag == NULL){ //no existe la pagina
					pag = crearPagina();
					agregarPagina(seg,pag);
					timestampActual = time(NULL);
					agregarDato(timestampActual, key, valor, pag);
					log_info(logger, "VOLVI DE AGREGAR DATO");
					pag->modificado = 1;
					log_info(logger,"\n\n\n\n tamanio de lista usos: %i \n\n\n\n\n\n", list_size(listaDeUsos));

				}else{ //existe la pagina
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
			pthread_mutex_lock(&lockTablaSeg);
			seg = crearSegmento(nombreTabla);
			pthread_mutex_lock(&seg->lockSegmento);
			list_add(tablaSegmentos, seg);
			pthread_mutex_unlock(&lockTablaSeg);
			pagina *pag = crearPagina();
			agregarPagina(seg,pag);
			timestampActual = time(NULL);
			agregarDato(timestampActual, key, valor, pag);
			log_info(logger, "VOLVI DE AGREGAR DATO");
			pag->modificado = 1;
			log_info(logger,"\n\n\n\n tamanio de lista usos: %i \n\n\n\n\n\n", list_size(listaDeUsos));

			pthread_mutex_unlock(&seg->lockSegmento);
		}
		log_info(logger, "Se inserto al segmento %s el valor %s", nombreTabla, valor);
		return 0;

	}
	else{
		log_info(logger,"La memoria esta FULL, no se puede hacer el INSERT");
		return 2;
	}
	}
	else{
		log_info(logger, "Se intentó insertar un value mayor al tamaño permitido");
		return 1;
	}

}



char* mSelect(char* nombreTabla,u_int16_t key){

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

			return valor;
		}
		else{
			if(!FULL()){
			pNueva = crearPagina();
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
				return valorPagNueva;
			}
				else{
					return noExiste;
				}
			}
			else{
				return estoyFull;
			}

		}
	}
	else{
		if(!FULL()){
		pthread_mutex_lock(&lockTablaSeg);
		nuevo = crearSegmento(nombreTabla);
		list_add(tablaSegmentos, nuevo);
		pthread_mutex_unlock(&lockTablaSeg);
		pNueva = crearPagina();
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
			return valorPagNueva;

		}
		else{
			return noExiste;
		}
		}
		else{
			return estoyFull;
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
	char* respuesta = string_from_format("%s%s", "0", rta);

	return respuesta;

}

int mDrop(char* nombreTabla){


	segmento* nuevo = buscarSegmento(nombreTabla);

	if(nuevo != NULL){
		pthread_mutex_lock(&lockTablaSeg);
		eliminarSegmento(nuevo);
		pthread_mutex_unlock(&lockTablaSeg);
		log_info(logger, "Se realizo un drop del segmento %s", nombreTabla);

	}
	int rta = dropLissandra(nombreTabla);

	return rta;
}


int mJournal(){
	log_info(logger, "Inicio del journal, se bloquea la tabla de segmentos \n\n\n\n\n\n\n\n\n\n\n");
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
				long timestamp = *(long*)conseguirTimestamp(pag);
				u_int16_t key = *(u_int16_t*)conseguirKey(pag);
				char* value = (char*)conseguirValor(pag);
				int insertExitoso = insertLissandra(nombreSegmento, timestamp, key, value);
				log_info(logger, "Rta insert LFS %d", insertExitoso);
				if(insertExitoso == 0){
						log_info(logger, "Se insertó correctamente el valor %s en la tabla %s", value, nombreSegmento);
					}
				else{
						log_info(logger, "No se pudo insertar el valor %s en la tabla %s", value, nombreSegmento);
					}
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
		log_info(logger, "Datos borrados, se desbloquea la tabla de segmentos");
		pthread_mutex_unlock(&lockTablaSeg);
		pthread_mutex_unlock(&lockTablaMarcos);

		return 0;
	}
	else{
		log_error(logger, "Algo salió mal al vaciar la lista de segmentos");
		return 1;
	}

}

//NROMEM;IP;PUERTO SUPER SEND CON TABLA ENTERA

char* formatearTablaGossip(int nro,char*ip,char*puerto){
	char* paquetin = string_from_format("%s;%s;%s;",string_itoa(nro),ip,puerto);
	return paquetin;
}


char* empaquetarTablaActivas(){
	int i=0;
	char* paquetote=string_new();
	infoMemActiva* aux = list_get(tablaMemActivas,i);

	while(i<(tablaMemActivas->elements_count)){
		string_append(&paquetote,formatearTablaGossip(aux->nroMem,aux->ip,aux->puerto));
		i++;
		aux = list_get(tablaMemActivas,i);
	}
	return paquetote;
}
void desempaquetarTablaSecundaria(char* paquete){
	int i=0;
	char** split = string_split(paquete,";");

	while(split[i]){
		infoMemActiva* aux = malloc(sizeof(infoMemActiva));
		aux->nroMem = atoi(split[i]);
		i++;
		aux->ip = split[i];
		i++;
		aux->puerto = split[i];
		i++;
		list_add(tablaMemActivasSecundaria,aux);
	}
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
	sendData(cliente,codOpe,sizeof(char)*2);
	recvData(cliente,rta,sizeof(char));
	char* tamTabla=malloc(sizeof(char)*4);
	recvData(cliente,tamTabla,sizeof(char)*3);
	char* buffer=malloc(atoi(tamTabla)+1);
	recvData(cliente,buffer,atoi(tamTabla));
	desempaquetarTablaSecundaria(buffer);  //aca actualizo mi tabla con lo que me envian

	char*paquete = confirmarActivo();
	sendData(cliente,paquete,strlen(paquete)+1);

	return 1;
} // devuelve si confirmo con 1 y recibe la tablaSecundaria y envio mi tabla

char* confirmarActivo(){ // podria recibir la ip y puerto del que pidio la confirmacion
   char* paquete=empaquetarTablaActivas();
   char* paqueteListo=empaquetar(0,paquete);
   return paqueteListo;
} //un listen y da el ok a otra mem al enviarle su tablaMemActivas //tendria que haber un hilo siempre escuchando

int estaRepetido(char*ip){
	int mismaIp(infoMemActiva* aux){
		return (strcmp(aux->ip,ip)==0);
	}
	return list_any_satisfy(tablaMemActivas,(void*)mismaIp); //Devuelve 1 si hay repetido
}

void actualizarArchConfig(char*ip,char*puerto){
	char* ipsAux = config_get_string_value(configuracion,"IP_SEEDS");
	char* seedsAux = config_get_string_value(configuracion,"PUERTO_SEEDS");

	char* newIps = string_new();
	char* newPuertos = string_new();

	string_append(&newIps,ipsAux);
	string_append(&newPuertos,seedsAux);

	string_append_with_format(&newIps,"%s;",ip);
	string_append_with_format(&newPuertos,"%s;",puerto);

	config_set_value(configuracion,"IP_SEEDS",newIps);
	config_set_value(configuracion,"PUERTO_SEEDS",newPuertos);
	config_save(configuracion);
}

void cargarInfoDeSecundaria(){
	int i=1; //la 0 ya la agregamos antes
	infoMemActiva*aux=list_get(tablaMemActivasSecundaria,i);
	while(aux){
		if(!estaRepetido(aux->ip)){
			list_add(tablaMemActivas,aux);//si NO esta repetido lo agrega y sino lo pasa de largo
			actualizarArchConfig(aux->ip,aux->puerto);
			}
		i++;
		aux=list_get(tablaMemActivasSecundaria,i);
	}
}


void agregarMemActiva(int id,char* ip,char*puerto){ //Se agrega a la lista //falta usar la secundaria y agregarle su info a la ppal y la config
	if(!estaRepetido(ip)){
	infoMemActiva* nueva = malloc(sizeof(infoMemActiva));
	nueva->ip=ip;
	nueva->puerto=puerto;
	nueva->nroMem=id;
	list_add(tablaMemActivas,nueva);
	actualizarArchConfig(ip,puerto); //preguntar que onda aca
	}
	cargarInfoDeSecundaria();
}

int conseguirIdSecundaria(){ //Devuelve el id de la memoria que confirmo que esta activa
	infoMemActiva* aux = list_get(tablaMemActivasSecundaria,0); //es la posicion cero porque como confirmo hay una nueva tabla secundaria
	return aux->nroMem;
}

void estaEnActivaElim(char*ip){ //Si estaba en la tablaMemActivas la elimina
    int i=1;
    infoMemActiva* aux = list_get(tablaMemActivas,i);
    while(aux){
        if(strcmp(aux->ip,ip)==0){
            list_remove(tablaMemActivas,i);
        }
        i++;
        aux = list_get(tablaMemActivas,i);
    }
}

void mGossip(){
	
    int i=1;
    char* ipsConfig = config_get_string_value(configuracion,"IP_SEEDS");
    char* seedsConfig = config_get_string_value(configuracion,"PUERTO_SEEDS");
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
}

